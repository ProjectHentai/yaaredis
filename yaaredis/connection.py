from packaging.version import Version
import sys
import asyncio
import inspect
import errno
import logging
import os
import socket
import time
from io import BytesIO

import yaaredis.compat
from yaaredis.exceptions import (AskError,
                                 AuthenticationFailureError,
                                 AuthenticationRequiredError,
                                 BusyLoadingError,
                                 AuthenticationWrongNumberOfArgsError,
                                 ModuleError,
                                 ClusterCrossSlotError,
                                 ClusterDownError,
                                 ConnectionError,  # pylint: disable=redefined-builtin
                                 ExecAbortError,
                                 InvalidResponse,
                                 MovedError,
                                 NoPermissionError,
                                 NoScriptError,
                                 ReadOnlyError,
                                 RedisError,
                                 ResponseError,
                                 TimeoutError,  # pylint: disable=redefined-builtin
                                 TryAgainError,
                                 DataError)
from yaaredis.utils import b, nativestr, HIREDIS_AVAILABLE

try:
    import ssl

    ssl_available = True
except ImportError:
    ssl_available = False

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {
    BlockingIOError: errno.EWOULDBLOCK,
}

if ssl_available:
    if hasattr(ssl, 'SSLWantReadError'):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())

if HIREDIS_AVAILABLE:
    import hiredis

    hiredis_version = Version(hiredis.__version__)
    HIREDIS_SUPPORTS_CALLABLE_ERRORS = \
        hiredis_version >= Version('0.1.3')
    HIREDIS_SUPPORTS_BYTE_BUFFER = \
        hiredis_version >= Version('0.1.4')
    HIREDIS_SUPPORTS_ENCODING_ERRORS = \
        hiredis_version >= Version('1.0.0')

    HIREDIS_USE_BYTE_BUFFER = True
    # only use byte buffer if hiredis supports it
    if not HIREDIS_SUPPORTS_BYTE_BUFFER:
        HIREDIS_USE_BYTE_BUFFER = False

SYM_STAR = b'*'
SYM_DOLLAR = b'$'
SYM_CRLF = b'\r\n'
SYM_EMPTY = b''

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

SENTINEL = object()
MODULE_LOAD_ERROR = 'Error loading the extension. ' \
                    'Please check the server logs.'
NO_SUCH_MODULE_ERROR = 'Error unloading module: no such module with that name'
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = 'Error unloading module: operation not ' \
                                   'possible.'
MODULE_EXPORTS_DATA_TYPES_ERROR = "Error unloading module: the module " \
                                  "exports one or more module-side data " \
                                  "types, can't unload"

logger = logging.getLogger(__name__)


async def exec_with_timeout(coroutine, timeout):
    try:
        return await asyncio.wait_for(coroutine, timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(exc) from exc


class Encoder:
    """Encode strings to bytes-like and decode bytes-like to strings"""

    def __init__(self, encoding, encoding_errors, decode_responses):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    def encode(self, value) -> bytes:
        """Return a bytestring or bytes-like representation of the value"""
        if isinstance(value, (bytes, memoryview)):
            return value
        elif isinstance(value, bool):
            # special case bool since it is a subclass of int
            raise DataError("Invalid input of type: 'bool'. Convert to a "
                            "bytes, string, int or float first.")
        elif isinstance(value, (int, float)):
            value = repr(value).encode()
        elif not isinstance(value, str):
            # a value we don't know how to deal with. throw an error
            typename = type(value).__name__
            raise DataError("Invalid input of type: '%s'. Convert to a "
                            "bytes, string, int or float first." % typename)
        if isinstance(value, str):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def decode(self, value, force=False):
        """Return a unicode string from the bytes-like representation"""
        if self.decode_responses or force:
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                value = value.decode(self.encoding, self.encoding_errors)
        return value


class SocketBuffer:
    def __init__(self, stream_reader, read_size):
        self._stream = stream_reader
        self.read_size = read_size
        self._buffer = BytesIO()
        # number of bytes written to the buffer from the socket
        self.bytes_written = 0
        # number of bytes read from the buffer
        self.bytes_read = 0

    @property
    def length(self):
        return self.bytes_written - self.bytes_read

    async def _read_from_socket(self, length=None):
        buf = self._buffer
        buf.seek(self.bytes_written)
        marker = 0

        try:
            while True:
                data = await self._stream.read(self.read_size)
                # an empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise ConnectionError('Socket closed on remote end')
                buf.write(data)
                data_length = len(data)
                self.bytes_written += data_length
                marker += data_length

                if length is not None and length > marker:
                    continue
                break
        except OSError as e:
            raise ConnectionError('Error reading from socket') from e

    async def read(self, length):
        length = length + 2  # make sure to read the \r\n terminator
        # make sure we've read enough data from the socket
        if length > self.length:
            await self._read_from_socket(length - self.length)

        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    async def readline(self):
        buf = self._buffer
        buf.seek(self.bytes_read)
        data = buf.readline()
        while not data.endswith(SYM_CRLF):
            # there's more data in the socket that we need
            await self._read_from_socket()
            buf.seek(self.bytes_read)
            data = buf.readline()

        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    def purge(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0

    def close(self):
        try:
            self.purge()
            self._buffer.close()
        except Exception:
            # issue #633 suggests the purge/close somehow raised a
            # BadFileDescriptor error. Perhaps the client ran out of
            # memory or something else? It's probably OK to ignore
            # any error being raised from purge/close since we're
            # removing the reference to the instance below.
            pass
        self._buffer = None


class BaseParser:
    EXCEPTION_CLASSES = {
        'ERR': {
            'max number of clients reached': ConnectionError,
            'Client sent AUTH, but no password is set': AuthenticationRequiredError,
            'invalid password': AuthenticationFailureError,
            # some Redis server versions report invalid command syntax
            # in lowercase
            'wrong number of arguments for \'auth\' command':
                AuthenticationWrongNumberOfArgsError,
            # some Redis server versions report invalid command syntax
            # in uppercase
            'wrong number of arguments for \'AUTH\' command':
                AuthenticationWrongNumberOfArgsError,
            MODULE_LOAD_ERROR: ModuleError,
            MODULE_EXPORTS_DATA_TYPES_ERROR: ModuleError,
            NO_SUCH_MODULE_ERROR: ModuleError,
            MODULE_UNLOAD_NOT_POSSIBLE_ERROR: ModuleError,
        },
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
        'READONLY': ReadOnlyError,
        'ASK': AskError,
        'TRYAGAIN': TryAgainError,
        'MOVED': MovedError,
        'CLUSTERDOWN': ClusterDownError,
        'CROSSSLOT': ClusterCrossSlotError,
        'WRONGPASS': AuthenticationFailureError,
        'NOAUTH': AuthenticationRequiredError,
        'NOPERM': NoPermissionError,
    }

    def parse_error(self, response):
        """Parse an error response"""
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            exception_class = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response)
        return ResponseError(response)


class PythonParser(BaseParser):
    def __init__(self, read_size):
        self._stream = None
        self._buffer = None
        self._read_size = read_size
        self.encoding = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        """Called when the stream connects"""
        # pylint: disable=protected-access
        self._stream = connection._reader
        self._buffer = SocketBuffer(self._stream, self._read_size)
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        """Called when the stream disconnects"""
        if self._stream is not None:
            self._stream = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoding = None

    def can_read(self):
        return self._buffer and bool(self._buffer.length)

    async def read_response(self):
        # pylint: disable=too-many-branches
        if not self._buffer:
            raise ConnectionError('Socket closed on remote end')
        response = await self._buffer.readline()
        if not response:
            raise ConnectionError('Socket closed on remote end')

        byte, response = chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse('Protocol Error: %s, %s' %
                                  (str(byte), str(response)))

        # server returned an error
        if byte == '-':
            response = response.decode()
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        if byte == '+':
            pass
        # int value
        elif byte == ':':
            response = int(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = await self._buffer.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = []
            for _ in range(length):
                response.append(await self.read_response())
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response


class HiredisParser(BaseParser):
    """Parser class for connections using Hiredis"""

    def __init__(self, read_size):
        if not HIREDIS_AVAILABLE:
            raise RedisError('Hiredis is not installed')
        self._stream = None
        self._reader = None
        self._read_size = read_size
        self._next_response = False

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def can_read(self):
        if not self._reader:
            raise ConnectionError('Socket closed on remote end')

        if self._next_response is False:
            self._next_response = self._reader.gets()
        return self._next_response is not False

    def on_connect(self, connection):
        # pylint: disable=protected-access
        self._stream = connection._reader
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': ResponseError,
        }
        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        if self._stream is not None:
            self._stream = None
        self._reader = None
        self._next_response = False

    async def read_response(self):
        if not self._stream:
            raise ConnectionError('Socket closed on remote end')

        # _next_response might be cached from a can_read() call
        if self._next_response is not False:
            response = self._next_response
            self._next_response = False
            return response

        response = self._reader.gets()
        while response is False:
            try:
                buffer = await self._stream.read(self._read_size)
            # CancelledError will be caught by client so that command won't be retried again
            # For more detailed discussion please see https://github.com/NoneGG/yaaredis/issues/56
            except yaaredis.compat.CancelledError:
                raise
            except Exception as e:
                raise ConnectionError('Error while reading from stream') from e

            if not buffer:
                raise ConnectionError('Socket closed on remote end')

            self._reader.feed(buffer)
            response = self._reader.gets()
        if isinstance(response, ResponseError):
            response = self.parse_error(response.args[0])
        return response


if HIREDIS_AVAILABLE:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser


class RedisSSLContext:
    def __init__(self, keyfile=None, certfile=None,
                 cert_reqs=None, ca_certs=None, check_hostname=False):
        self.keyfile = keyfile
        self.certfile = certfile
        if cert_reqs is None:
            self.cert_reqs = ssl.CERT_NONE
        elif isinstance(cert_reqs, str):
            CERT_REQS = {
                'none': ssl.CERT_NONE,
                'optional': ssl.CERT_OPTIONAL,
                'required': ssl.CERT_REQUIRED,
            }
            if cert_reqs not in CERT_REQS:
                raise RedisError(
                    'Invalid SSL Certificate Requirements Flag: %s' %
                    cert_reqs)
            self.cert_reqs = CERT_REQS[cert_reqs]
        self.ca_certs = ca_certs
        self.context = None
        self.check_hostname = check_hostname

    def get(self):
        if not self.keyfile:
            self.context = ssl.create_default_context(cafile=self.ca_certs)
        else:
            self.context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self.context.verify_mode = self.cert_reqs
            self.context.load_cert_chain(certfile=self.certfile,
                                         keyfile=self.keyfile)
            self.context.load_verify_locations(self.ca_certs)
        self.context.check_hostname = self.check_hostname
        self.context.verify_mode = self.cert_reqs
        return self.context


class BaseConnection:
    # pylint: disable=too-many-instance-attributes
    description = 'BaseConnection'

    def __init__(self, retry_on_timeout=False, stream_timeout=None,
                 parser_class=DefaultParser, reader_read_size=65535,
                 encoding='utf-8', encoding_errors='strict', decode_responses=False, health_check_interval=0,
                 *, client_name=None, loop=None):
        self._parser = parser_class(reader_read_size)
        self._stream_timeout = stream_timeout
        self._reader = None  # type: asyncio.StreamReader
        self._writer = None  # type: asyncio.StreamWriter
        self.username = ''
        self.password = ''
        self.db = ''
        self.pid = os.getpid()
        self.retry_on_timeout = retry_on_timeout
        self._description_args = {}
        self._connect_callbacks = []
        self.encoding = encoding
        self.decode_responses = decode_responses
        self.loop = loop
        self.client_name = client_name
        # flag to show if a connection is waiting for response
        self.awaiting_response = False
        self.health_check_interval = health_check_interval  # todo this and retry
        self.next_health_check = 0
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self.last_active_at = time.time()
        self._buffer_cutoff = 6000

    def __repr__(self):
        return self.description.format(**self._description_args)

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    @property
    def is_connected(self):
        return bool(self._reader and self._writer)

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    async def can_read(self):
        """Checks for data that can be read"""
        if not self.is_connected:
            await self.connect()
        return self._parser.can_read()

    async def connect(self):
        try:
            await self._connect()
        except (TimeoutError, yaaredis.compat.CancelledError):
            raise
        except Exception as e:
            e = sys.exc_info()[1]
            raise ConnectionError("Error during initial connection: %s" % (e.args,)) from e
        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            task = callback(self)
            # typing.Awaitable is not available in Python3.5
            # so use inspect.isawaitable instead
            # according to issue https://github.com/NoneGG/yaaredis/issues/77
            if inspect.isawaitable(task):
                await task

    async def _connect(self):
        raise NotImplementedError

    async def on_connect(self):
        self._parser.on_connect(self)

        # if a username and a password is specified, authenticate
        if self.username and self.password:
            await self.send_command('AUTH', self.username, self.password)
            if nativestr(await self.read_response()) != 'OK':
                raise ConnectionError('Failed to set username or password')
        # if a password is specified, authenticate
        elif self.password:
            await self.send_command('AUTH', self.password)
            if nativestr(await self.read_response()) != 'OK':
                raise ConnectionError('Failed to set password')

        # if a database is specified, switch to it
        if self.db:
            await self.send_command('SELECT', self.db)
            if nativestr(await self.read_response()) != 'OK':
                raise ConnectionError('Invalid Database')

        if self.client_name is not None:
            await self.send_command('CLIENT SETNAME', self.client_name)
            if nativestr(await self.read_response()) != 'OK':
                raise ConnectionError('Failed to set client name')

        self.last_active_at = time.time()

    async def read_response(self):
        try:
            response = await exec_with_timeout(self._parser.read_response(), self._stream_timeout)
            self.last_active_at = time.time()
        except TimeoutError:
            self.disconnect()
            raise
        if isinstance(response, RedisError):
            raise response
        self.awaiting_response = False
        return response

    async def send_packed_command(self, command):
        """Sends an already packed command to the Redis server"""
        if not self._writer:
            await self.connect()
        try:
            if isinstance(command, str):
                command = [command]
            self._writer.writelines(command)
        except yaaredis.compat.TimeoutError as e:
            self.disconnect()
            raise TimeoutError('Timeout writing to socket') from e
        except Exception as e:
            self.disconnect()
            if len(e.args) == 1:
                errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                errno = e.args[0]
                errmsg = e.args[1]
            raise ConnectionError('Error %s while writing to socket. %s.' %
                                  (errno, errmsg)) from e
        except BaseException:
            self.disconnect()
            raise

    async def send_command(self, *args):
        if not self.is_connected:
            await self.connect()
        await self.send_packed_command(self.pack_command(*args))
        self.awaiting_response = True
        self.last_active_at = time.time()

    def encode(self, value):
        """Returns a bytestring representation of the value"""
        if isinstance(value, bytes):
            return value

        if isinstance(value, int):
            value = b(str(value))
        elif isinstance(value, float):
            value = b(repr(value))
        elif not isinstance(value, str):
            value = str(value)

        if isinstance(value, str):
            value = value.encode(self.encoding)

        return value

    def disconnect(self):
        """Disconnects from the Redis server"""
        self._parser.on_disconnect()
        try:
            self._writer.close()
        except Exception:
            pass
        self._reader = None
        self._writer = None

    def pack_command(self, *args):
        """Pack a series of arguments into the Redis protocol"""
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b' ' in args[0]:
            args = tuple(args[0].split()) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

        buffer_cutoff = self._buffer_cutoff
        for arg in map(self.encoder.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values or memoryviews
            arg_length = len(arg)
            if (len(buff) > buffer_cutoff or arg_length > buffer_cutoff
                    or isinstance(arg, memoryview)):
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(),
                     SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def pack_commands(self, commands):
        'Pack multiple commands into the Redis protocol'
        output = []
        pieces = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                pieces.append(chunk)
                buffer_length += len(chunk)

            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output


class Connection(BaseConnection):
    # pylint: disable=too-many-instance-attributes
    description = 'Connection<host={host},port={port},db={db}>'

    def __init__(self, host='127.0.0.1', port=6379, username=None, password=None,
                 db=0, retry_on_timeout=False, stream_timeout=None, connect_timeout=None,
                 ssl_context=None, parser_class=DefaultParser, reader_read_size=65535,
                 encoding='utf-8', encoding_errors="strict", decode_responses=False, socket_keepalive=None,
                 socket_keepalive_options=None, *, client_name=None, loop=None):
        # pylint: disable=too-many-locals
        super().__init__(retry_on_timeout, stream_timeout,
                         parser_class, reader_read_size,
                         encoding, encoding_errors, decode_responses,
                         client_name=client_name, loop=loop)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db = db
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
        }
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}

    async def _connect(self):
        reader, writer = await exec_with_timeout(
            asyncio.open_connection(host=self.host,
                                    port=self.port,
                                    ssl=self.ssl_context),
            self._connect_timeout,
        )
        self._reader = reader
        self._writer = writer
        sock = writer.transport.get_extra_info('socket')
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            except (OSError, TypeError):
                # `socket_keepalive_options` might contain invalid options
                # causing an error. Do not leave the connection open.
                writer.close()
                raise
        await self.on_connect()


class UnixDomainSocketConnection(BaseConnection):
    # pylint: disable=too-many-instance-attributes
    description = 'UnixDomainSocketConnection<path={path},db={db}>'

    def __init__(self, path='', username=None, password=None,
                 db=0, retry_on_timeout=False, stream_timeout=None, connect_timeout=None,
                 ssl_context=None, parser_class=DefaultParser, reader_read_size=65535,
                 encoding='utf-8', decode_responses=False, *, client_name=None, loop=None):
        super().__init__(retry_on_timeout, stream_timeout,
                         parser_class, reader_read_size,
                         encoding, decode_responses,
                         client_name=client_name, loop=loop)
        self.path = path
        self.db = db
        self.username = username
        self.password = password
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args = {
            'path': self.path,
            'db': self.db,
        }

    async def _connect(self):
        reader, writer = await exec_with_timeout(
            asyncio.open_unix_connection(path=self.path,
                                         ssl=self.ssl_context),
            self._connect_timeout,
        )
        self._reader = reader
        self._writer = writer
        await self.on_connect()


class ClusterConnection(Connection):
    """Manages TCP communication to and from a Redis server"""
    description = 'ClusterConnection<host={host},port={port}>'

    def __init__(self, *args, **kwargs):
        self.readonly = kwargs.pop('readonly', False)
        super().__init__(*args, **kwargs)

    async def on_connect(self):
        """
        Initialize the connection, authenticate and select a database and send READONLY if it is
        set during object initialization.
        """
        if self.db:
            logger.error('SELECT DB is not allowed in cluster mode')
            self.db = ''

        await super().on_connect()
        if self.readonly:
            await self.send_command('READONLY')
            if nativestr(await self.read_response()) != 'OK':
                raise ConnectionError('READONLY command failed')
