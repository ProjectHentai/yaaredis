from yaaredis.utils import bool_ok, nativestr, NodeFlag


class ConnectionCommandMixin:
    RESPONSE_CALLBACKS = {
        'AUTH': bool,
        'PING': lambda r: nativestr(r) == 'PONG',
        'SELECT': bool_ok,
        'QUIT': bool_ok
    }

    async def echo(self, value):
        """Echo the string back from the server"""
        return await self.execute_command('ECHO', value)

    async def hello(self):
        pass  # todo https://redis.io/commands/hello

    async def ping(self):
        """Ping the Redis server"""
        return await self.execute_command('PING')

    async def quit(self):
        """
        Ask the server to close the connection.

        For more information check https://redis.io/commands/quit
        """
        return await self.execute_command('QUIT')

    async def reset(self):
        return await self.execute_command('RESET')

    async def select(self, index):
        return await self.execute_command('SELECT', index)


class ClusterConnectionCommandMixin(ConnectionCommandMixin):
    NODES_FLAGS = {
        'PING': NodeFlag.ALL_NODES,
        'ECHO': NodeFlag.ALL_NODES,
    }

    RESULT_CALLBACKS = {
        'ECHO': lambda res: res,
        'PING': lambda res: res,
    }
