from yaaredis.exceptions import DataError
from yaaredis.utils import str_if_bytes, bool_ok, pairs_to_dict


def list_or_args(keys, args):
    # returns a single new list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (bytes, str)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


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


class ACLCommandMixin:
    RESPONSE_CALLBACKS = {
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
    }

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
