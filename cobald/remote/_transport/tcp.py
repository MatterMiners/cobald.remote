import trio
from typing import AsyncIterator, Optional, Dict, Tuple, Any


from cobald.remote._abc import Transport, Connector
from cobald.remote.utility import async_iter


def int2bytes(num: int) -> bytes:
    return num.to_bytes((num.bit_length() + 7) // 8, byteorder='big')


def bytes2int(num: bytes) -> int:
    return int.from_bytes(num, byteorder='big')


class TcpConnector(Connector):
    __instances__ = {}  # type: Dict[Tuple[Any, int], TcpConnector]

    def __new__(cls, host, port: int):
        try:
            instance = cls.__instances__[host, port]
        except KeyError:
            instance = cls.__instances__[host, port] = super().__new__(cls)
        return instance

    def __init__(self, host, port: int):
        if hasattr(self, 'host'):
            return
        self.host = host
        self.port = port
        self._listening = False
        self._acceptor = None  # type: Optional[TcpAcceptor]

    async def accept(self) -> 'TcpTransport':
        async with self as acceptor:
            stream = await acceptor.__anext__()
        return TcpTransport(stream)

    async def connect(self) -> 'TcpTransport':
        stream = await trio.open_tcp_stream(self.host, self.port)
        return TcpTransport(stream)

    async def __aenter__(self) -> 'TcpAcceptor':
        if self._listening:
            raise RuntimeError('%s is not re-entrant' % self)
        self._listening = True
        self._acceptor = TcpAcceptor(await trio.open_tcp_listeners(port=self.port, host=self.host))
        return self._acceptor

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._acceptor.listener.aclose()
        self._listening = False
        return False

    def __repr__(self):
        return '<%s remote=%s, listening=%s>' % (self.__class__.__name__, (self.host, self.port), self._listening)


class TcpAcceptor:
    def __init__(self, listener: trio.SocketListener):
        self.listener = listener

    async def __anext__(self) -> trio.SocketStream:
        return await self.listener.accept()

    def __aiter__(self):
        return self


class TcpTransport(Transport):
    def __init__(self, stream: trio.SocketStream):
        super().__init__()
        self.stream = stream
        self._remote = None

    async def put(self, *messages: bytes):
        for message in messages:
            await self._put_one(message)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return async_iter(self._get_one())

    # the message format is <payload length>:4 <payload>
    # in other words, the length is encoded in the first 4 bytes
    # this is enough to support
    # - 256 ** 4 - 1 bytes (4 GiB) per message
    async def _put_one(self, message: bytes):
        """Send a single message"""
        assert len(message) < 256 ** 4 - 1, 'message too large for %s' % self
        await self.stream.send_all(int2bytes(len(message)) + message)

    async def _get_one(self) -> bytes:
        """Read a single message"""
        length = bytes2int(await self._receive_exactly(4))
        message = await self._receive_exactly(length)
        return message

    async def _receive_exactly(self, byte_count: int) -> bytes:
        """Read exactly ``byte_count`` bytes"""
        message = await self.stream.receive_some(byte_count)
        while len(message) < byte_count:
            chunk = await self._receive_exactly(8)
            if not chunk:
                await self.stream.aclose()
                raise RuntimeError(
                    'Transport %s closed while reading %d/%d outstanding bytes' % (
                        self.stream, byte_count - len(message), byte_count
                    ))
            message += chunk
        return message

    def __str__(self):
        return '%s<%s -> %s>' % (
            self.__class__.__name__, self.stream.socket.getsockname(), self.stream.socket.getpeername()
        )

    def __repr__(self):
        return '<%s local=%s, remote=%s>' % (
            self.__class__.__name__, self.stream.socket.getsockname(), self.stream.socket.getpeername()
        )
