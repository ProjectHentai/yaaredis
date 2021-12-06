from redis.commands.timeseries import TimeSeries as _TimeSeries, TimeSeriesCommands

from yaaredis.pipeline import StrictPipeline


class TimeSeries(_TimeSeries):
    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the TimeSeries module, that can be used
        for executing only TimeSeries commands and core commands.

        Usage example:

        r = redis.Redis()
        pipe = r.ts().pipeline()
        for i in range(100):
            pipeline.add("with_pipeline", i, 1.1 * i)
        pipeline.execute()

        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.MODULE_CALLBACKS,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        return p


class Pipeline(TimeSeriesCommands, StrictPipeline):
    """Pipeline for the module."""
