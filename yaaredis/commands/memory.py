# -*- coding: utf-8 -*-
from yaaredis.utils import str_if_bytes, pairs_to_dict


def bool_ok(response):
    return str_if_bytes(response) == 'OK'


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


class MemoryCommandMixin:
    RESPONSE_CALLBACKS = {
        'MEMORY PURGE': bool_ok,
        'MEMORY STATS': parse_memory_stats,
        'MEMORY USAGE': int_or_none,
    }

    async def memory_doctor(self):
        """
        The MEMORY DOCTOR command reports about different memory-related issues that the Redis server experiences,
        and advises about possible remedies.
        """
        return await self.execute_command('MEMORY DOCTOR')

    async def memory_help(self):
        return await self.execute_command('MEMORY HELP')

    # todo MEMORY PURGE https://redis.io/commands/memory-purge
