from yaaredis.monitor import Monitor


class MonitorCommandMixin:
    RESPONSE_CALLBACKS = {}

    async def monitor(self):
        """
        todo add monitor command
        >>>     async with await c.monitor() as m:
                    async for command in m:
                        print(command)
        """
        return await Monitor.new(self.connection_pool)
