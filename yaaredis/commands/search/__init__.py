import time

from redis.commands.search import Search as _Search
from redis.commands.search.commands import SUGADD_COMMAND, SYNDUMP_CMD, INFO_CMD, SEARCH_CMD, AGGREGATE_CMD, CURSOR_CMD, \
    SPELLCHECK_CMD, CONFIG_CMD, SUGGET_COMMAND, FUZZY, WITHSCORES, WITHPAYLOADS
from redis.commands.search._util import to_string
from redis.commands.search.result import Result
from redis.commands.search.aggregation import AggregateRequest, AggregateResult, Cursor
from redis.commands.search.suggestion import SuggestionParser


class Search(_Search):
    async def info(self):
        """
        Get info an stats about the the current index, including the number of
        documents, memory consumption, etc
        """

        res = await self.client.execute_command(INFO_CMD, self.index_name)
        it = map(to_string, res)
        return dict(zip(it, it))

    async def search(self, query):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with
                     default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format
        """
        args, query = self._mk_query_args(query)
        st = time.time()
        res = await self.execute_command(SEARCH_CMD, *args)

        return Result(
            res,
            not query._no_content,
            duration=(time.time() - st) * 1000.0,
            has_payload=query._with_payloads,
            with_scores=query._with_scores,
        )

    async def aggregate(self, query):
        """
        Issue an aggregation query

        ### Parameters

        **query**: This can be either an `AggeregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from
        its `rows` property, which will always yield the rows of the result.
        """
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)

        raw = await self.execute_command(*cmd)
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = raw[1]
                cursor = query
            else:
                cursor = Cursor(raw[1])
            raw = raw[0]
        else:
            cursor = None

        if isinstance(query, AggregateRequest) and query._with_schema:
            schema = raw[0]
            rows = raw[2:]
        else:
            schema = None
            rows = raw[1:]

        res = AggregateResult(rows, cursor, schema)
        return res

    async def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        ### Parameters

        **query**: search query.
        **distance***: the maximal Levenshtein distance for spelling
                       suggestions (default: 1, max: 4).
        **include**: specifies an inclusion custom dictionary.
        **exclude**: specifies an exclusion custom dictionary.
        """
        cmd = [SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(["DISTANCE", distance])

        if include:
            cmd.extend(["TERMS", "INCLUDE", include])

        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        raw = await self.execute_command(*cmd)

        corrections = {}
        if raw == 0:
            return corrections

        for _correction in raw:
            if isinstance(_correction, int) and _correction == 0:
                continue

            if len(_correction) != 3:
                continue
            if not _correction[2]:
                continue
            if not _correction[2][0]:
                continue

            # For spellcheck output
            # 1)  1) "TERM"
            #     2) "{term1}"
            #     3)  1)  1)  "{score1}"
            #             2)  "{suggestion1}"
            #         2)  1)  "{score2}"
            #             2)  "{suggestion2}"
            #
            # Following dictionary will be made
            # corrections = {
            #     '{term1}': [
            #         {'score': '{score1}', 'suggestion': '{suggestion1}'},
            #         {'score': '{score2}', 'suggestion': '{suggestion2}'}
            #     ]
            # }
            corrections[_correction[1]] = [
                {"score": _item[0], "suggestion": _item[1]}
                for _item in _correction[2]
            ]

        return corrections

    async def config_set(self, option, value):
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.
        """
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = await self.execute_command(*cmd)
        return raw == "OK"

    async def config_get(self, option):
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.
        """
        cmd = [CONFIG_CMD, "GET", option]
        res = {}
        raw = await self.execute_command(*cmd)
        if raw:
            for kvs in raw:
                res[kvs[0]] = kvs[1]
        return res

    async def sugadd(self, key, *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has
        a score and string.
        If kwargs["increment"] is true and the terms are already in the
        server's dictionary, we increment their scores.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftsugadd>`_.  # noqa
        """
        # If Transaction is not False it will MULTI/EXEC which will error
        pipe = self.pipeline(transaction=False)
        for sug in suggestions:
            args = [SUGADD_COMMAND, key, sug.string, sug.score]
            if kwargs.get("increment"):
                args.append("INCR")
            if sug.payload:
                args.append("PAYLOAD")
                args.append(sug.payload)

            await pipe.execute_command(*args)

        return (await pipe.execute())[-1]

    async def sugget(
            self, key, prefix, fuzzy=False, num=10, with_scores=False,
            with_payloads=False
    ):
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix.
        More information `here <https://oss.redis.com/redisearch/master/Commands/#ftsugget>`_.  # noqa

        Parameters:

        prefix : str
            The prefix we are searching. **Must be valid ascii or utf-8**
        fuzzy : bool
            If set to true, the prefix search is done in fuzzy mode.
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes
            can be very
            slow, and even scan the entire index.
        with_scores : bool
            If set to true, we also return the (refactored) score of
            each suggestion.
            This is normally not needed, and is NOT the original score
            inserted into the index.
        with_payloads : bool
            Return suggestion payloads
        num : int
            The maximum number of results we return. Note that we might
            return less. The algorithm trims irrelevant suggestions.

        Returns:

        list:
             A list of Suggestion objects. If with_scores was False, the
             score of all suggestions is 1.
        """
        args = [SUGGET_COMMAND, key, prefix, "MAX", num]
        if fuzzy:
            args.append(FUZZY)
        if with_scores:
            args.append(WITHSCORES)
        if with_payloads:
            args.append(WITHPAYLOADS)

        ret = await self.execute_command(*args)
        results = []
        if not ret:
            return results

        parser = SuggestionParser(with_scores, with_payloads, ret)
        return [s for s in parser]

    async def syndump(self):
        """
        Dumps the contents of a synonym group.

        The command is used to dump the synonyms data structure.
        Returns a list of synonym terms and their synonym group ids.
        """
        raw = await self.execute_command(SYNDUMP_CMD, self.index_name)
        return {raw[i]: raw[i + 1] for i in range(0, len(raw), 2)}
