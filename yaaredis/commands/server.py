import datetime

from yaaredis.exceptions import RedisError, DataError
from yaaredis.utils import (b,
                            bool_ok,
                            dict_merge,
                            list_keys_to_dict,
                            nativestr,
                            NodeFlag,
                            pairs_to_dict,
                            string_keys_to_dict,
                            str_if_bytes)


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


class ServerCommandMixin:
    # pylint: disable=too-many-public-methods
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict('BGREWRITEAOF BGSAVE', lambda r: True),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB SAVE '
            'SHUTDOWN SLAVEOF', bool_ok,
        ),
        {
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
        },
    )

    async def bgrewriteaof(self):
        """Tell the Redis server to rewrite the AOF file from data in memory"""
        return await self.execute_command('BGREWRITEAOF')

    async def bgsave(self):
        """
        Tells the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return await self.execute_command('BGSAVE')

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

    async def client_list(self):
        """Returns a list of currently connected clients"""
        return await self.execute_command('CLIENT LIST')

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
