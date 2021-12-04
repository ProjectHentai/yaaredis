from yaaredis.utils import bool_ok, dict_merge, list_keys_to_dict, nativestr, NodeFlag

SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'config-epoch': int,
    'down-after-milliseconds': int,
    'failover-timeout': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'last-ping-sent': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'parallel-syncs': int,
    'port': int,
    'quorum': int,
    'role-reported-time': int,
    's-down-time': int,
    'slave-priority': int,
    'slave-repl-offset': int,
    'voted-leader-epoch': int,
}


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except Exception:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result['flags'].split(','))
    for name, flag in (('is_master', 'master'), ('is_slave', 'slave'),
                       ('is_sdown', 's_down'), ('is_odown', 'o_down'),
                       ('is_sentinel', 'sentinel'),
                       ('is_disconnected', 'disconnected'),
                       ('is_master_down', 'master_down')):
        result[name] = flag in flags
    return result


def parse_sentinel_master(response):
    return parse_sentinel_state(map(nativestr, response))


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = parse_sentinel_state(map(nativestr, item))
        result[state['name']] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [parse_sentinel_state(map(nativestr, item)) for item in response]


def parse_sentinel_get_master(response):
    return response and (response[0], int(response[1])) or None


class SentinelCommandMixin:
    RESPONSE_CALLBACKS = {
        'SENTINEL GET-MASTER-ADDR-BY-NAME': parse_sentinel_get_master,
        'SENTINEL MASTER': parse_sentinel_master,
        'SENTINEL MASTERS': parse_sentinel_masters,
        'SENTINEL MONITOR': bool_ok,
        'SENTINEL REMOVE': bool_ok,
        'SENTINEL SENTINELS': parse_sentinel_slaves_and_sentinels,
        'SENTINEL SET': bool_ok,
        'SENTINEL SLAVES': parse_sentinel_slaves_and_sentinels,
        'SENTINEL CKQUORUM': bool_ok,
        'SENTINEL FAILOVER': bool_ok,
        'SENTINEL FLUSHCONFIG': bool_ok,
        'SENTINEL RESET': bool_ok
    }

    async def sentinel_get_master_addr_by_name(self, service_name):
        """Returns a (host, port) pair for the given ``service_name``"""
        return await self.execute_command('SENTINEL GET-MASTER-ADDR-BY-NAME',
                                          service_name)

    async def sentinel_master(self, service_name):
        """Returns a dictionary containing the specified masters state."""
        return await self.execute_command('SENTINEL MASTER', service_name)

    async def sentinel_masters(self):
        """Returns a list of dictionaries containing each master's state."""
        return await self.execute_command('SENTINEL MASTERS')

    async def sentinel_monitor(self, name, ip, port, quorum):
        """Adds a new master to Sentinel to be monitored"""
        return await self.execute_command('SENTINEL MONITOR', name, ip, port, quorum)

    async def sentinel_remove(self, name):
        """Removes a master from Sentinel's monitoring"""
        return await self.execute_command('SENTINEL REMOVE', name)

    async def sentinel_sentinels(self, service_name):
        """Returns a list of sentinels for ``service_name``"""
        return await self.execute_command('SENTINEL SENTINELS', service_name)

    async def sentinel_set(self, name, option, value):
        """Sets Sentinel monitoring parameters for a given master"""
        return await self.execute_command('SENTINEL SET', name, option, value)

    async def sentinel_slaves(self, service_name):
        """Returns a list of slaves for ``service_name``"""
        return await self.execute_command('SENTINEL SLAVES', service_name)

    async def sentinel_reset(self, pattern):
        """
        This command will reset all the masters with matching name.
        The pattern argument is a glob-style pattern.

        The reset process clears any previous state in a master (including a
        failover in progress), and removes every slave and sentinel already
        discovered and associated with the master.
        """
        return await self.execute_command('SENTINEL RESET', pattern, once=True)

    async def sentinel_failover(self, new_master_name):
        """
        Force a failover as if the master was not reachable, and without
        asking for agreement to other Sentinels (however a new version of the
        configuration will be published so that the other Sentinels will
        update their configurations).
        """
        return await self.execute_command('SENTINEL FAILOVER', new_master_name)

    async def sentinel_ckquorum(self, new_master_name):
        """
        Check if the current Sentinel configuration is able to reach the
        quorum needed to failover a master, and the majority needed to
        authorize the failover.

        This command should be used in monitoring systems to check if a
        Sentinel deployment is ok.
        """
        return await self.execute_command('SENTINEL CKQUORUM',
                                          new_master_name,
                                          once=True)

    async def sentinel_flushconfig(self):
        """
        Force Sentinel to rewrite its configuration on disk, including the
        current Sentinel state.

        Normally Sentinel rewrites the configuration every time something
        changes in its state (in the context of the subset of the state which
        is persisted on disk across restart).
        However sometimes it is possible that the configuration file is lost
        because of operation errors, disk failures, package upgrade scripts or
        configuration managers. In those cases a way to to force Sentinel to
        rewrite the configuration file is handy.

        This command works even if the previous configuration file is
        completely missing.
        """
        return await self.execute_command('SENTINEL FLUSHCONFIG')


class ClusterSentinelCommands(SentinelCommandMixin):
    NODES_FLAGS = dict_merge(
        list_keys_to_dict(
            ['SENTINEL GET-MASTER-ADDR-BY-NAME', 'SENTINEL MASTER', 'SENTINEL MASTERS',
             'SENTINEL MONITOR', 'SENTINEL REMOVE', 'SENTINEL SENTINELS', 'SENTINEL SET',
             'SENTINEL SLAVES'], NodeFlag.BLOCKED,
        ),
    )
