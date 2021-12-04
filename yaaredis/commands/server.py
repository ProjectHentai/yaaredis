import datetime

from yaaredis.monitor import Monitor
from yaaredis.exceptions import RedisError, DataError, ModuleError
from yaaredis.utils import (b,
                            list_or_args,
                            bool_ok,
                            dict_merge,
                            list_keys_to_dict,
                            nativestr,
                            NodeFlag,
                            pairs_to_dict,
                            string_keys_to_dict,
                            str_if_bytes)


def parse_acl_log(response, **options):
    if response is None:
        return None
    if isinstance(response, list):
        data = []
        for log in response:
            log_data = pairs_to_dict(log, True, True)
            client_info = log_data.get('client-info', '')
            log_data["client-info"] = parse_client_info(client_info)

            # float() is lossy comparing to the "double" in C
            log_data["age-seconds"] = float(log_data["age-seconds"])
            data.append(log_data)
    else:
        data = bool_ok(response)
    return data


def parse_acl_getuser(response, **options):
    if response is None:
        return None
    data = pairs_to_dict(response, decode_keys=True)

    # convert everything but user-defined data in 'keys' to native strings
    data['flags'] = list(map(str_if_bytes, data['flags']))
    data['passwords'] = list(map(str_if_bytes, data['passwords']))
    data['commands'] = str_if_bytes(data['commands'])

    # split 'commands' into separate 'categories' and 'commands' lists
    commands, categories = [], []
    for command in data['commands'].split(' '):
        if '@' in command:
            categories.append(command)
        else:
            commands.append(command)

    data['commands'] = commands
    data['categories'] = categories
    data['enabled'] = 'on' in data['flags']
    return data


def parse_memory_stats(response, **kwargs):
    """Parse the results of MEMORY STATS"""
    stats = pairs_to_dict(response,
                          decode_keys=True,
                          decode_string_values=True)
    for key, value in stats.items():
        if key.startswith('db.'):
            stats[key] = pairs_to_dict(value,
                                       decode_keys=True,
                                       decode_string_values=True)
    return stats


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def parse_slowlog_get(response, **_options):
    return [{
        'id': item[0],
        'start_time': int(item[1]),
        'duration': int(item[2]),
        'command': b(' ').join(item[3]),
    } for item in response]


def parse_client_list(response, **options):
    clients = []
    for c in str_if_bytes(response).splitlines():
        # Values might contain '='
        clients.append(dict(pair.split('=', 1) for pair in c.split(' ')))
    return clients


def parse_client_info(value):
    """
    Parsing client-info in ACL Log in following format.
    "key1=value1 key2=value2 key3=value3"
    """
    client_info = {}
    infos = str_if_bytes(value).split(" ")
    for info in infos:
        key, value = info.split("=")
        client_info[key] = value

    # Those fields are defined as int in networking.c
    for int_key in {"id", "age", "idle", "db", "sub", "psub",
                    "multi", "qbuf", "qbuf-free", "obl",
                    "argv-mem", "oll", "omem", "tot-mem"}:
        client_info[int_key] = int(client_info[int_key])
    return client_info


def parse_config_get(response, **_options):
    response = [nativestr(i) if i is not None else None for i in response]
    return pairs_to_dict(response) if response else {}


def timestamp_to_datetime(response):
    """Converts a unix timestamp to a Python datetime object"""
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def parse_debug_object(response):
    """Parse the results of Redis's DEBUG OBJECT command into a Python dict"""
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = str_if_bytes(response)
    response = 'type:' + response
    response = dict(kv.split(':') for kv in response.split())

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ('refcount', 'serializedlength', 'lru', 'lru_seconds_idle')
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_info(response):
    """Parses the result of Redis's INFO command into a Python dict"""
    info = {}
    response = nativestr(response)

    def get_value(value):
        if ',' not in value or '=' not in value:
            try:
                if '.' in value:
                    return float(value)
                return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(','):
                k, v = item.rsplit('=', 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            if line.find(':') != -1:
                key, value = line.split(':', 1)
                info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault('__raw__', []).append(line)

    return info


def parse_role(response):
    role = nativestr(response[0])

    def _parse_master(response):
        offset, slaves = response[1:]
        res = {
            'role': role,
            'offset': offset,
            'slaves': [],
        }
        for slave in slaves:
            host, port, offset = slave
            res['slaves'].append({
                'host': host,
                'port': int(port),
                'offset': int(offset),
            })
        return res

    def _parse_slave(response):
        host, port, status, offset = response[1:]
        return {
            'role': role,
            'host': host,
            'port': port,
            'status': status,
            'offset': offset,
        }

    def _parse_sentinel(response):
        return {
            'role': role,
            'masters': response[1:],
        }

    parser = {
        'master': _parse_master,
        'slave': _parse_slave,
        'sentinel': _parse_sentinel,
    }[role]
    return parser(response)


def parse_client_kill(response, **options):
    if isinstance(response, int):
        return response
    return str_if_bytes(response) == 'OK'


def parse_module_result(response):
    if isinstance(response, ModuleError):
        raise response
    return True


class ServerCommandMixin:
    # pylint: disable=too-many-public-methods
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict('BGREWRITEAOF BGSAVE', lambda r: True),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB SAVE '
            'SHUTDOWN SLAVEOF', bool_ok,
        ),
        {
            'ACL CAT': lambda r: list(map(str_if_bytes, r)),
            'ACL DELUSER': int,
            'ACL GENPASS': str_if_bytes,
            'ACL GETUSER': parse_acl_getuser,
            'ACL HELP': lambda r: list(map(str_if_bytes, r)),
            'ACL LIST': lambda r: list(map(str_if_bytes, r)),
            'ACL LOAD': bool_ok,
            'ACL LOG': parse_acl_log,
            'ACL SAVE': bool_ok,
            'ACL SETUSER': bool_ok,
            'ACL USERS': lambda r: list(map(str_if_bytes, r)),
            'ACL WHOAMI': str_if_bytes,
            'ROLE': parse_role,
            'SLOWLOG GET': parse_slowlog_get,
            'SLOWLOG LEN': int,
            'SLOWLOG RESET': bool_ok,
            'CLIENT ID': int,
            'CLIENT KILL': parse_client_kill,
            'CLIENT LIST': parse_client_list,
            'CLIENT INFO': parse_client_info,
            'CLIENT SETNAME': bool_ok,
            'CLIENT UNBLOCK': lambda r: r and int(r) == 1 or False,
            'CLIENT PAUSE': bool_ok,
            'CLIENT GETREDIR': int,
            'CLIENT TRACKINGINFO': lambda r: list(map(str_if_bytes, r)),
            'CONFIG GET': parse_config_get,
            'CONFIG RESETSTAT': bool_ok,
            'CONFIG SET': bool_ok,
            'DEBUG OBJECT': parse_debug_object,
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'TIME': lambda x: (int(x[0]), int(x[1])),
            'MEMORY PURGE': bool_ok,
            'MEMORY STATS': parse_memory_stats,
            'MEMORY USAGE': int_or_none,
            'MODULE LOAD': parse_module_result,
            'MODULE UNLOAD': parse_module_result,
            'MODULE LIST': lambda r: [pairs_to_dict(m) for m in r],
        },
    )

    async def acl_cat(self, category=None):
        """
        Returns a list of categories or commands within a category.

        If ``category`` is not supplied, returns a list of all categories.
        If ``category`` is supplied, returns a list of all commands within
        that category.

        For more information check https://redis.io/commands/acl-cat
        """
        pieces = [category] if category else []
        return await self.execute_command('ACL CAT', *pieces)

    async def acl_deluser(self, *username):
        """
        Delete the ACL for the specified ``username``s

        For more information check https://redis.io/commands/acl-deluser
        """
        return await self.execute_command('ACL DELUSER', *username)

    async def acl_genpass(self, bits=None):
        """Generate a random password value.
        If ``bits`` is supplied then use this number of bits, rounded to
        the next multiple of 4.
        See: https://redis.io/commands/acl-genpass
        """
        pieces = []
        if bits is not None:
            try:
                b = int(bits)
                if b < 0 or b > 4096:
                    raise ValueError
            except ValueError:
                raise DataError('genpass optionally accepts a bits argument, '
                                'between 0 and 4096.')
        return await self.execute_command('ACL GENPASS', *pieces)

    async def acl_getuser(self, username):
        """
        Get the ACL details for the specified ``username``.

        If ``username`` does not exist, return None

        For more information check https://redis.io/commands/acl-getuser
        """
        return await self.execute_command('ACL GETUSER', username)

    async def acl_help(self):
        """The ACL HELP command returns helpful text describing
        the different subcommands.

        For more information check https://redis.io/commands/acl-help
        """
        return await self.execute_command('ACL HELP')

    async def acl_list(self):
        """
        Return a list of all ACLs on the server

        For more information check https://redis.io/commands/acl-list
        """
        return await self.execute_command('ACL LIST')

    async def acl_log(self, count=None):
        """
        Get ACL logs as a list.
        :param int count: Get logs[0:count].
        :rtype: List.

        For more information check https://redis.io/commands/acl-log
        """
        args = []
        if count is not None:
            if not isinstance(count, int):
                raise DataError('ACL LOG count must be an '
                                'integer')
            args.append(count)

        return await self.execute_command('ACL LOG', *args)

    async def acl_log_reset(self):
        """
        Reset ACL logs.
        :rtype: Boolean.

        For more information check https://redis.io/commands/acl-log
        """
        args = [b'RESET']
        return await self.execute_command('ACL LOG', *args)

    async def acl_load(self):
        """
        Load ACL rules from the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to load ACL rules from an aclfile.

        For more information check https://redis.io/commands/acl-load
        """
        return await self.execute_command('ACL LOAD')

    async def acl_save(self):
        """
        Save ACL rules to the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to save ACL rules to an aclfile.

        For more information check https://redis.io/commands/acl-save
        """
        return await self.execute_command('ACL SAVE')

    async def acl_setuser(self, username, enabled=False, nopass=False,
                          passwords=None, hashed_passwords=None, categories=None,
                          commands=None, keys=None, reset=False, reset_keys=False,
                          reset_passwords=False):
        """
        Create or update an ACL user.

        Create or update the ACL for ``username``. If the user already exists,
        the existing ACL is completely overwritten and replaced with the
        specified values.

        ``enabled`` is a boolean indicating whether the user should be allowed
        to authenticate or not. Defaults to ``False``.

        ``nopass`` is a boolean indicating whether the can authenticate without
        a password. This cannot be True if ``passwords`` are also specified.

        ``passwords`` if specified is a list of plain text passwords
        to add to or remove from the user. Each password must be prefixed with
        a '+' to add or a '-' to remove. For convenience, the value of
        ``passwords`` can be a simple prefixed string when adding or
        removing a single password.

        ``hashed_passwords`` if specified is a list of SHA-256 hashed passwords
        to add to or remove from the user. Each hashed password must be
        prefixed with a '+' to add or a '-' to remove. For convenience,
        the value of ``hashed_passwords`` can be a simple prefixed string when
        adding or removing a single password.

        ``categories`` if specified is a list of strings representing category
        permissions. Each string must be prefixed with either a '+' to add the
        category permission or a '-' to remove the category permission.

        ``commands`` if specified is a list of strings representing command
        permissions. Each string must be prefixed with either a '+' to add the
        command permission or a '-' to remove the command permission.

        ``keys`` if specified is a list of key patterns to grant the user
        access to. Keys patterns allow '*' to support wildcard matching. For
        example, '*' grants access to all keys while 'cache:*' grants access
        to all keys that are prefixed with 'cache:'. ``keys`` should not be
        prefixed with a '~'.

        ``reset`` is a boolean indicating whether the user should be fully
        reset prior to applying the new ACL. Setting this to True will
        remove all existing passwords, flags and privileges from the user and
        then apply the specified rules. If this is False, the user's existing
        passwords, flags and privileges will be kept and any new specified
        rules will be applied on top.

        ``reset_keys`` is a boolean indicating whether the user's key
        permissions should be reset prior to applying any new key permissions
        specified in ``keys``. If this is False, the user's existing
        key permissions will be kept and any new specified key permissions
        will be applied on top.

        ``reset_passwords`` is a boolean indicating whether to remove all
        existing passwords and the 'nopass' flag from the user prior to
        applying any new passwords specified in 'passwords' or
        'hashed_passwords'. If this is False, the user's existing passwords
        and 'nopass' status will be kept and any new specified passwords
        or hashed_passwords will be applied on top.

        For more information check https://redis.io/commands/acl-setuser
        """
        encoder = self.connection_pool.get_encoder()
        pieces = [username]

        if reset:
            pieces.append(b'reset')

        if reset_keys:
            pieces.append(b'resetkeys')

        if reset_passwords:
            pieces.append(b'resetpass')

        if enabled:
            pieces.append(b'on')
        else:
            pieces.append(b'off')

        if (passwords or hashed_passwords) and nopass:
            raise DataError('Cannot set \'nopass\' and supply '
                            '\'passwords\' or \'hashed_passwords\'')

        if passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            passwords = list_or_args(passwords, [])
            for i, password in enumerate(passwords):
                password = encoder.encode(password)
                if password.startswith(b'+'):
                    pieces.append(b'>%s' % password[1:])
                elif password.startswith(b'-'):
                    pieces.append(b'<%s' % password[1:])
                else:
                    raise DataError('Password %d must be prefixeed with a '
                                    '"+" to add or a "-" to remove' % i)

        if hashed_passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            hashed_passwords = list_or_args(hashed_passwords, [])
            for i, hashed_password in enumerate(hashed_passwords):
                hashed_password = encoder.encode(hashed_password)
                if hashed_password.startswith(b'+'):
                    pieces.append(b'#%s' % hashed_password[1:])
                elif hashed_password.startswith(b'-'):
                    pieces.append(b'!%s' % hashed_password[1:])
                else:
                    raise DataError('Hashed %d password must be prefixeed '
                                    'with a "+" to add or a "-" to remove' % i)

        if nopass:
            pieces.append(b'nopass')

        if categories:
            for category in categories:
                category = encoder.encode(category)
                # categories can be prefixed with one of (+@, +, -@, -)
                if category.startswith(b'+@'):
                    pieces.append(category)
                elif category.startswith(b'+'):
                    pieces.append(b'+@%s' % category[1:])
                elif category.startswith(b'-@'):
                    pieces.append(category)
                elif category.startswith(b'-'):
                    pieces.append(b'-@%s' % category[1:])
                else:
                    raise DataError('Category "%s" must be prefixed with '
                                    '"+" or "-"'
                                    % encoder.decode(category, force=True))
        if commands:
            for cmd in commands:
                cmd = encoder.encode(cmd)
                if not cmd.startswith(b'+') and not cmd.startswith(b'-'):
                    raise DataError('Command "%s" must be prefixed with '
                                    '"+" or "-"'
                                    % encoder.decode(cmd, force=True))
                pieces.append(cmd)

        if keys:
            for key in keys:
                key = encoder.encode(key)
                pieces.append(b'~%s' % key)

        return await self.execute_command('ACL SETUSER', *pieces)

    async def acl_users(self):
        """Returns a list of all registered users on the server.

        For more information check https://redis.io/commands/acl-users
        """
        return await self.execute_command('ACL USERS')

    async def acl_whoami(self):
        """Get the username for the current connection

        For more information check https://redis.io/commands/acl-whoami
        """
        return await self.execute_command('ACL WHOAMI')

    async def bgrewriteaof(self):
        """Tell the Redis server to rewrite the AOF file from data in memory"""
        return await self.execute_command('BGREWRITEAOF')

    async def bgsave(self, schedule=True):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.

        For more information check https://redis.io/commands/bgsave
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command('BGSAVE', *pieces)

    async def client_kill(self, address):
        """Disconnects the client at ``address`` (ip:port)"""
        return await self.execute_command('CLIENT KILL', address)

    async def client_info(self):
        """
        Returns information and statistics about the current
        client connection.

        For more information check https://redis.io/commands/client-info
        """
        return await self.execute_command('CLIENT INFO')

    async def client_list(self, _type=None, client_id=[]):
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. a list of client ids

        For more information check https://redis.io/commands/client-list
        """
        args = []
        if _type is not None:
            client_types = ('normal', 'master', 'replica', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT LIST _type must be one of %r" % (
                    client_types,))
            args.append(b'TYPE')
            args.append(_type)
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id != []:
            args.append(b"ID")
            args.append(' '.join(client_id))
        return await self.execute_command('CLIENT LIST', *args)

    async def client_getname(self):
        """Returns the current connection name"""
        return await self.execute_command('CLIENT GETNAME')

    async def client_setname(self, name):
        """Sets the current connection name"""
        return await self.execute_command('CLIENT SETNAME', name)

    async def client_unblock(self, client_id, error=False):
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.

        For more information check https://redis.io/commands/client-unblock
        """
        args = ['CLIENT UNBLOCK', int(client_id)]
        if error:
            args.append(b'ERROR')
        return await self.execute_command(*args)

    async def client_getredir(self):
        """
        Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return await self.execute_command('CLIENT GETREDIR')

    async def client_reply(self, reply):
        """
        Enable and disable redis server replies.
        ``reply`` Must be ON OFF or SKIP,
            ON - The default most with server replies to commands
            OFF - Disable server responses to commands
            SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
              The test_client_reply unit test illustrates this, and
              conftest.py has a client with a timeout.

        See https://redis.io/commands/client-reply
        """
        replies = ['ON', 'OFF', 'SKIP']
        if reply not in replies:
            raise DataError('CLIENT REPLY must be one of %r' % replies)
        return await self.execute_command("CLIENT REPLY", reply)

    async def client_id(self):
        """
        Returns the current connection id

        For more information check https://redis.io/commands/client-id
        """
        return await self.execute_command('CLIENT ID')

    async def client_trackinginfo(self):
        """
        Returns the information about the current client connection's
        use of the server assisted client side cache.

        See https://redis.io/commands/client-trackinginfo
        """
        return await self.execute_command('CLIENT TRACKINGINFO')

    async def client_pause(self, timeout=0):
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients

        For more information check https://redis.io/commands/client-pause
        """
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        return await self.execute_command('CLIENT PAUSE', timeout)

    async def client_unpause(self):
        """
        Unpause all redis clients

        For more information check https://redis.io/commands/client-unpause
        """
        return await self.execute_command('CLIENT UNPAUSE')

    async def config_get(self, pattern='*'):
        """Returns a dictionary of configuration based on the ``pattern``"""
        return await self.execute_command('CONFIG GET', pattern)

    async def config_set(self, name, value):
        """Sets config item ``name`` to ``value``"""
        return await self.execute_command('CONFIG SET', name, value)

    async def config_resetstat(self):
        """Resets runtime statistics"""
        return await self.execute_command('CONFIG RESETSTAT')

    async def config_rewrite(self):
        """
        Rewrites config file with the minimal change to reflect running config
        """
        return await self.execute_command('CONFIG REWRITE')

    async def dbsize(self):
        """Returns the number of keys in the current database"""
        return await self.execute_command('DBSIZE')

    async def debug_object(self, key):
        """Returns version specific meta information about a given key"""
        return await self.execute_command('DEBUG OBJECT', key)

    async def debug_segfault(self):
        return await self.execute_command('DEBUG SEGFAULT')

    async def flushall(self):
        """Deletes all keys in all databases on the current host"""
        return await self.execute_command('FLUSHALL')

    async def flushdb(self, asynchronous=False):
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information check https://redis.io/commands/flushdb
        """
        args = []
        if asynchronous:
            args.append(b'ASYNC')
        return await self.execute_command('FLUSHDB', *args)

    async def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        if section is None:
            return await self.execute_command('INFO')
        return await self.execute_command('INFO', section)

    async def lastsave(self):
        """
        Returns a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return await self.execute_command('LASTSAVE')

    async def save(self):
        """
        Tells the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return await self.execute_command('SAVE')

    async def memory_doctor(self):
        """
        The MEMORY DOCTOR command reports about different memory-related issues that the Redis server experiences,
        and advises about possible remedies.
        """
        return await self.execute_command('MEMORY DOCTOR')

    async def memory_help(self):
        return await self.execute_command('MEMORY HELP')

    async def memory_malloc_stats(self):
        return await self.execute_command('MEMORY MALLOC-STATS')

    async def memory_purge(self):
        return await self.execute_command('MEMORY PURGE')

    async def memory_stats(self):
        return await self.execute_command('MEMORY STATS')

    async def memory_usage(self, key, samples=None):
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.

        For more information check https://redis.io/commands/memory-usage
        """
        args = []
        if isinstance(samples, int):
            args.extend([b'SAMPLES', samples])
        return await self.execute_command('MEMORY USAGE', key, *args)

    async def module_load(self, path, *args):
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.

        For more information check https://redis.io/commands/module-load
        """
        return await self.execute_command('MODULE LOAD', path, *args)

    async def module_unload(self, name):
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.

        For more information check https://redis.io/commands/module-unload
        """
        return await self.execute_command('MODULE UNLOAD', name)

    async def module_list(self):
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.

        For more information check https://redis.io/commands/module-list
        """
        return await self.execute_command('MODULE LIST')

    async def monitor(self):
        """
        todo add monitor command
        >>>     async with await c.monitor() as m:
                    async for command in m:
                        print(command)
        """
        return await Monitor.new(self.connection_pool)

    async def shutdown(self):
        """Stops Redis server"""
        try:
            await self.execute_command('SHUTDOWN')
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError('SHUTDOWN seems to have failed.')

    async def slaveof(self, host=None, port=None):
        """
        Sets the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.
        """
        if host is None and port is None:
            return await self.execute_command('SLAVEOF', b('NO'), b('ONE'))
        return await self.execute_command('SLAVEOF', host, port)

    async def slowlog_get(self, num=None):
        """
        Gets the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args = ['SLOWLOG GET']
        if num is not None:
            args.append(num)
        return await self.execute_command(*args)

    async def slowlog_len(self):
        """Gets the number of items in the slowlog"""
        return await self.execute_command('SLOWLOG LEN')

    async def slowlog_reset(self):
        """Removes all items in the slowlog"""
        return await self.execute_command('SLOWLOG RESET')

    async def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return await self.execute_command('TIME')

    async def role(self):
        """
        Provides information on the role of a Redis instance in the context of replication,
        by returning if the instance is currently a master, slave, or sentinel.
        The command also returns additional information about the state of the replication
        (if the role is master or slave)
        or the list of monitored master names (if the role is sentinel).
        :return:
        """
        return await self.execute_command('ROLE')

    async def lolwut(self, *version_numbers):
        """
        Get the Redis version and a piece of generative computer art

        See: https://redis.io/commands/lolwut
        """
        if version_numbers:
            return await self.execute_command('LOLWUT VERSION', *version_numbers)
        else:
            return await self.execute_command('LOLWUT')


class ClusterServerCommandMixin(ServerCommandMixin):
    NODES_FLAGS = dict_merge(
        list_keys_to_dict(
            ['SHUTDOWN', 'SLAVEOF', 'CLIENT SETNAME'],
            NodeFlag.BLOCKED,
        ),
        list_keys_to_dict(
            ['FLUSHALL', 'FLUSHDB'],
            NodeFlag.ALL_MASTERS,
        ),
        list_keys_to_dict(
            ['SLOWLOG LEN', 'SLOWLOG RESET', 'SLOWLOG GET',
             'TIME', 'SAVE', 'LASTSAVE', 'DBSIZE',
             'CONFIG RESETSTAT', 'CONFIG REWRITE',
             'CONFIG GET', 'CONFIG SET', 'CLIENT KILL',
             'CLIENT LIST', 'CLIENT GETNAME', 'INFO',
             'BGSAVE', 'BGREWRITEAOF'],
            NodeFlag.ALL_NODES,
        ),
    )

    RESULT_CALLBACKS = dict_merge(
        list_keys_to_dict(
            ['CONFIG GET', 'CONFIG SET', 'SLOWLOG GET',
             'CLIENT KILL', 'INFO', 'BGREWRITEAOF',
             'BGSAVE', 'CLIENT LIST', 'CLIENT GETNAME',
             'CONFIG RESETSTAT', 'CONFIG REWRITE', 'DBSIZE',
             'LASTSAVE', 'SAVE', 'SLOWLOG LEN',
             'SLOWLOG RESET', 'TIME', 'FLUSHALL',
             'FLUSHDB'],
            lambda res: res,
        ),
    )
