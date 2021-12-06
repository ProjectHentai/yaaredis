from redis.commands.json import JSON as _JSON, JSONCommands
from redis.commands.json.path import Path

from yaaredis.pipeline import StrictPipeline


class JSON(_JSON):
    async def get(self, name, *args, no_escape=False):
        """
        Get the object stored as a JSON value at key ``name``.

        ``args`` is zero or more paths, and defaults to root path
        ```no_escape`` is a boolean flag to add no_escape option to get
        non-ascii characters
        """
        pieces = [name]
        if no_escape:
            pieces.append("noescape")

        if len(args) == 0:
            pieces.append(Path.rootPath())

        else:
            for p in args:
                pieces.append(str(p))

        # Handle case where key doesn't exist. The JSONDecoder would raise a
        # TypeError exception since it can't decode None
        try:
            return await self.execute_command("JSON.GET", *pieces)
        except TypeError:
            return None

    def pipeline(self, transaction=True, shard_hint=None):
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.MODULE_CALLBACKS,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        p._encode = self._encode
        p._decode = self._decode
        return p


class Pipeline(JSONCommands, StrictPipeline):
    pass
