import re
import asyncio

from yaaredis.exceptions import RedisError
from yaaredis.utils import bool_ok
from yaaredis.pool import ConnectionPool


# todo add monitor like redis-py


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """
    monitor_re = re.compile(r'\[(\d+) (.*)\] (.*)')
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self.connection = self.connection_pool.get_connection('MONITOR')

    @classmethod
    async def new(cls, connection_pool: ConnectionPool):
        self = cls(connection_pool)
        if asyncio.iscoroutine(self.connection):
            self.connection = await self.connection
        return self

    async def __aenter__(self):
        await self.connection.send_command('MONITOR')
        # check that monitor returns 'OK', but don't return it to user
        response = await self.connection.read_response()
        if not bool_ok(response):
            raise RedisError('MONITOR failed: %s' % response)
        return self

    async def __aexit__(self, *args):
        self.connection.disconnect()
        self.connection_pool.release(self.connection)

    async def next_command(self):
        """Parse the response from a monitor command"""
        response = await self.connection.read_response()
        if isinstance(response, bytes):
            response: str = self.connection.encoder.decode(response, force=True)
        command_time, command_data = response.split(' ', 1)
        m = self.monitor_re.match(command_data)
        db_id, client_info, command = m.groups()
        command = ' '.join(self.command_re.findall(command))
        # Redis escapes double quotes because each piece of the command
        # string is surrounded by double quotes. We don't have that
        # requirement so remove the escaping and leave the quote.
        command = command.replace('\\"', '"')

        if client_info == 'lua':
            client_address = 'lua'
            client_port = ''
            client_type = 'lua'
        elif client_info.startswith('unix'):
            client_address = 'unix'
            client_port = client_info[5:]
            client_type = 'unix'
        else:
            # use rsplit as ipv6 addresses contain colons
            client_address, client_port = client_info.rsplit(':', 1)
            client_type = 'tcp'
        return {
            'time': float(command_time),
            'db': int(db_id),
            'client_address': client_address,
            'client_port': client_port,
            'client_type': client_type,
            'command': command
        }

    async def listen(self):
        """Listen for commands coming to the server."""
        while True:
            yield await self.next_command()

    __aiter__ = listen
