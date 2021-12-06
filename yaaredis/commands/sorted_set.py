# pylint: disable=redefined-builtin
from yaaredis.exceptions import RedisError, DataError
from yaaredis.utils import b, dict_merge, first_key, int_or_none, string_keys_to_dict, list_or_args, float_or_none

# todo need functions and check callbacs
VALID_ZADD_OPTIONS = {'NX', 'XX', 'CH', 'INCR'}





def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options.get('withscores'):
        return response
    score_cast_func = options.get('score_cast_func', float)
    it = iter(response)
    return list(zip(it, map(score_cast_func, it)))


def parse_zadd(response, **options):
    if response is None:
        return None
    if options.get('as_score'):
        return float(response)
    return int(response)


def parse_zscan(response, **options):
    score_cast_func = options.get('score_cast_func', float)
    cursor, r = response
    it = iter(r)
    return int(cursor), list(zip(it, map(score_cast_func, it)))


def parse_zmscore(response, **options):
    # zmscore: list of scores (double precision floating point number) or nil
    return [float(score) if score is not None else None for score in response]


class SortedSetCommandMixin:
    # pylint: disable=too-many-public-methods

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'ZCARD ZLEXCOUNT '
            'ZREM ZREMRANGEBYLEX '
            'ZREMRANGEBYRANK '
            'ZREMRANGEBYSCORE', int,
        ),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZPOPMAX ZPOPMIN ZINTER ZDIFF ZUNION',
            zset_score_pairs,
        ),
        string_keys_to_dict('ZRANK ZREVRANK', int_or_none),
        {
            'ZSCAN': parse_zscan,
            'ZADD': parse_zadd,
            'ZMSCORE': parse_zmscore
        },
        string_keys_to_dict('BZPOPMIN BZPOPMAX',
                            lambda r: r and (r[0], r[1], float(r[2])) or None)
    )

    async def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False,
             gt=None, lt=None):
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        ``LT`` Only update existing elements if the new score is less than
        the current score. This flag doesn't prevent adding new elements.

        ``GT`` Only update existing elements if the new score is greater than
        the current score. This flag doesn't prevent adding new elements.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.

        ``NX``, ``LT``, and ``GT`` are mutually exclusive options.

        See: https://redis.io/commands/ZADD
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError("ZADD option 'incr' only works when passing a "
                            "single element/score pair")
        if nx is True and (gt is not None or lt is not None):
            raise DataError("Only one of 'nx', 'lt', or 'gr' may be defined.")

        pieces = []
        options = {}
        if nx:
            pieces.append(b'NX')
        if xx:
            pieces.append(b'XX')
        if ch:
            pieces.append(b'CH')
        if incr:
            pieces.append(b'INCR')
            options['as_score'] = True
        if gt:
            pieces.append(b'GT')
        if lt:
            pieces.append(b'LT')
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return await self.execute_command('ZADD', name, *pieces, **options)

    async def zaddoption(
            self, name, option=None,
            *args, **kwargs):  # pylint: disable=keyword-arg-before-vararg
        """
        Differs from zadd in that you can set either 'XX' or 'NX' option as
        described here: https://redis.io/commands/zadd. Only for Redis 3.0.2 or
        later.

        The following example would add four values to the 'my-key' key:
        redis.zaddoption('my-key', 'XX', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        redis.zaddoption('my-key', 'NX CH', name1=2.2)
        """
        if not option:
            raise RedisError('ZADDOPTION must take options')
        options = {opt.upper() for opt in option.split()}
        if options - VALID_ZADD_OPTIONS:
            raise RedisError('ZADD only takes XX, NX, CH, or INCR')
        if 'NX' in options and 'XX' in options:
            raise RedisError('ZADD only takes one of XX or NX')
        pieces = list(options)
        members = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError('ZADD requires an equal number of '
                                 'values and scores')
            members.extend(args)
        for pair in iter(kwargs.items()):
            members.append(pair[1])
            members.append(pair[0])
        if 'INCR' in options and len(members) != 2:
            raise RedisError('ZADD with INCR only takes one score-name pair')
        return await self.execute_command('ZADD', name, *pieces, *members)

    async def zcard(self, name):
        """Returns the number of elements in the sorted set ``name``"""
        return await self.execute_command('ZCARD', name)

    async def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return await self.execute_command('ZCOUNT', name, min, max)

    async def zdiff(self, keys, withscores=False):
        """
        Returns the difference between the first and all successive input
        sorted sets provided in ``keys``.

        For more information check https://redis.io/commands/zdiff
        """
        pieces = [len(keys), *keys]
        if withscores:
            pieces.append("WITHSCORES")
        return await self.execute_command("ZDIFF", *pieces)

    async def zdiffstore(self, dest, keys):
        """
        Computes the difference between the first and all successive input
        sorted sets provided in ``keys`` and stores the result in ``dest``.

        For more information check https://redis.io/commands/zdiffstore
        """
        pieces = [len(keys), *keys]
        return await self.execute_command("ZDIFFSTORE", dest, *pieces)

    async def zincrby(self, name, amount, value):
        """
        Increment the score of ``value`` in sorted set ``name`` by ``amount``

        For more information check https://redis.io/commands/zincrby
        """
        return await self.execute_command('ZINCRBY', name, amount, value)

    async def zinter(self, keys, aggregate=None, withscores=False):
        """
        Return the intersect of multiple sorted sets specified by ``keys``.
        With the ``aggregate`` option, it is possible to specify how the
        results of the union are aggregated. This option defaults to SUM,
        where the score of an element is summed across the inputs where it
        exists. When this option is set to either MIN or MAX, the resulting
        set will contain the minimum or maximum score of an element across
        the inputs where it exists.

        For more information check https://redis.io/commands/zinter
        """
        return await self._zaggregate('ZINTER', None, keys, aggregate,
                                      withscores=withscores)

    async def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersects multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return await self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    async def zlexcount(self, name, min, max):
        """
        Returns the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return await self.execute_command('ZLEXCOUNT', name, min, max)

    async def zrange(self, name, start, end, desc=False, withscores=False,
                     score_cast_func=float, byscore=False, bylex=False,
                     offset=None, num=None):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrange
        """
        # Need to support ``desc`` also when using old redis version
        # because it was supported in 3.5.3 (of redis-py)
        if not byscore and not bylex and (offset is None and num is None) \
                and desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)

        return await self._zrange('ZRANGE', None, name, start, end, desc, byscore,
                                  bylex, withscores, score_cast_func, offset, num)

    async def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Returns the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if ((start is not None and num is None)
                or (num is not None and start is None)):
            raise RedisError('``start`` and ``num`` must both be specified')
        pieces = ['ZRANGEBYLEX', name, min, max]
        if start is not None and num is not None:
            pieces.extend([b('LIMIT'), start, num])
        return await self.execute_command(*pieces)

    async def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Returns the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if ((start is not None and num is None)
                or (num is not None and start is None)):
            raise RedisError('``start`` and ``num`` must both be specified')
        pieces = ['ZREVRANGEBYLEX', name, max, min]
        if start is not None and num is not None:
            pieces.extend([b('LIMIT'), start, num])
        return await self.execute_command(*pieces)

    async def zrangebyscore(self, name, min, max, start=None, num=None,
                            withscores=False, score_cast_func=float):
        """
        Returns a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if ((start is not None and num is None)
                or (num is not None and start is None)):
            raise RedisError('``start`` and ``num`` must both be specified')
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend([b('LIMIT'), start, num])
        if withscores:
            pieces.append(b('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
        }
        return await self.execute_command(*pieces, **options)

    async def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return await self.execute_command('ZRANK', name, value)

    async def zrem(self, name, *values):
        """Removes member ``values`` from sorted set ``name``"""
        return await self.execute_command('ZREM', name, *values)

    async def zremrangebylex(self, name, min, max):
        """
        Removes all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return await self.execute_command('ZREMRANGEBYLEX', name, min, max)

    async def zremrangebyrank(self, name, min, max):
        """
        Removes all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return await self.execute_command('ZREMRANGEBYRANK', name, min, max)

    async def zremrangebyscore(self, name, min, max):
        """
        Removes all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return await self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    async def zrevrange(self, name, start, end, withscores=False,
                        score_cast_func=float):
        """
        Returns a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append(b('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
        }
        return await self.execute_command(*pieces, **options)

    async def zrevrangebyscore(self, name, max, min, start=None, num=None,
                               withscores=False, score_cast_func=float):
        """
        Returns a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if ((start is not None and num is None)
                or (num is not None and start is None)):
            raise RedisError('``start`` and ``num`` must both be specified')
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend([b('LIMIT'), start, num])
        if withscores:
            pieces.append(b('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
        }
        return await self.execute_command(*pieces, **options)

    async def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return await self.execute_command('ZREVRANK', name, value)

    async def zscore(self, name, value):
        """Return the score of element ``value`` in sorted set ``name``"""
        return await self.execute_command('ZSCORE', name, value)

    async def zunion(self, keys, aggregate=None, withscores=False):
        """
        Return the union of multiple sorted sets specified by ``keys``.
        ``keys`` can be provided as dictionary of keys and their weights.
        Scores will be aggregated based on the ``aggregate``, or SUM if
        none is provided.

        For more information check https://redis.io/commands/zunion
        """
        return await self._zaggregate('ZUNION', None, keys, aggregate,
                                      withscores=withscores)

    async def zunionstore(self, dest, keys, aggregate=None):
        """
        Performs Union on multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return await self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    async def zmscore(self, key, members):
        """
        Returns the scores associated with the specified members
        in the sorted set stored at key.
        ``members`` should be a list of the member name.
        Return type is a list of score.
        If the member does not exist, a None will be returned
        in corresponding position.

        For more information check https://redis.io/commands/zmscore
        """
        if not members:
            raise DataError('ZMSCORE members must be a non-empty list')
        pieces = [key] + members
        return await self.execute_command('ZMSCORE', *pieces)

    async def _zrange(self, command, dest, name, start, end, desc=False,
                      byscore=False, bylex=False, withscores=False,
                      score_cast_func=float, offset=None, num=None):
        if byscore and bylex:
            raise DataError("``byscore`` and ``bylex`` can not be "
                            "specified together.")
        if (offset is not None and num is None) or \
                (num is not None and offset is None):
            raise DataError("``offset`` and ``num`` must both be specified.")
        if bylex and withscores:
            raise DataError("``withscores`` not supported in combination "
                            "with ``bylex``.")
        pieces = [command]
        if dest:
            pieces.append(dest)
        pieces.extend([name, start, end])
        if byscore:
            pieces.append('BYSCORE')
        if bylex:
            pieces.append('BYLEX')
        if desc:
            pieces.append('REV')
        if offset is not None and num is not None:
            pieces.extend(['LIMIT', offset, num])
        if withscores:
            pieces.append('WITHSCORES')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return await self.execute_command(*pieces, **options)

    async def _zaggregate(self, command, dest, keys, aggregate=None,
                          **options):
        pieces = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.append(len(keys))
        if isinstance(keys, dict):
            keys, weights = keys.keys(), keys.values()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(b'WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            if aggregate.upper() in ['SUM', 'MIN', 'MAX']:
                pieces.append(b'AGGREGATE')
                pieces.append(aggregate)
            else:
                raise DataError("aggregate can be sum, min or max.")
        if options.get('withscores', False):
            pieces.append(b'WITHSCORES')
        return await self.execute_command(*pieces, **options)

    async def zscan(self, name, cursor=0, match=None, count=None,
                    score_cast_func=float):
        """
        Incrementally returns lists of elements in a sorted set. Also returns
        a cursor pointing to the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([b('MATCH'), match])
        if count is not None:
            pieces.extend([b('COUNT'), count])
        options = {'score_cast_func': score_cast_func}
        return await self.execute_command('ZSCAN', *pieces, **options)

    async def zpopmax(self, name, count=None):
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmax
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return await self.execute_command('ZPOPMAX', name, *args, **options)

    async def zpopmin(self, name, count=None):
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmin
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return await self.execute_command('ZPOPMIN', name, *args, **options)

    async def zrandmember(self, key, count=None, withscores=False):
        """
        Return a random element from the sorted set value stored at key.

        ``count`` if the argument is positive, return an array of distinct
        fields. If called with a negative count, the behavior changes and
        the command is allowed to return the same field multiple times.
        In this case, the number of returned fields is the absolute value
        of the specified count.

        ``withscores`` The optional WITHSCORES modifier changes the reply so it
        includes the respective scores of the randomly selected elements from
        the sorted set.

        For more information check https://redis.io/commands/zrandmember
        """
        params = []
        if count is not None:
            params.append(count)
        if withscores:
            params.append("WITHSCORES")

        return await self.execute_command("ZRANDMEMBER", key, *params)

    async def bzpopmax(self, keys, timeout=0):
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmax
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return await self.execute_command('BZPOPMAX', *keys)

    async def bzpopmin(self, keys, timeout=0):
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmin
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return await self.execute_command('BZPOPMIN', *keys)

    async def zmpop(self, numkeys, keys, **kwargs):
        keys = list_or_args(keys)
        pass  # todo

    async def bzmpop(self, timeout, numkeys, keys, **kwargs):
        """
        https://redis.io/commands/bzmpop
        """
        keys = list_or_args(keys)
        pass

    async def zrangestore(self, dest, name, start, end,
                          byscore=False, bylex=False, desc=False,
                          offset=None, num=None):
        """
        Stores in ``dest`` the result of a range of values from sorted set
        ``name`` between ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrangestore
        """
        return await self._zrange('ZRANGESTORE', dest, name, start, end, desc,
                                  byscore, bylex, False, None, offset, num)


class ClusterSortedSetCommandMixin(SortedSetCommandMixin):
    RESULT_CALLBACKS = {
        'ZSCAN': first_key,
    }
