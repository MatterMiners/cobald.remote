from typing import AsyncIterator, AsyncContextManager

import trio
from async_generator import asynccontextmanager

from .._interface.api import Transport
from .._interface.streams import MessageStream, StreamClosed, StreamBroken
from ..utility import async_iter


class TCPTransport(Transport):
    __slots__ = ('host', 'port')

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __connect__(self) -> 'AsyncContextManager[MessageStream]':
        stream = await trio.open_tcp_stream(host=self.host, port=self.port)
        return scope_stream(stream)

    async def __accept__(self) -> 'AsyncIterator[AsyncContextManager[MessageStream]]':
        listeners = await trio.open_tcp_listeners(host=self.host, port=self.port)
        while True:
            async for connection in async_iter(*listeners):
                yield scope_stream(connection)


@asynccontextmanager
async def scope_stream(stream: trio.SocketStream) -> AsyncContextManager[MessageStream]:
    async with stream:
        tcp_stream = TCPStream(stream)
        yield tcp_stream


class TCPStream(MessageStream):
    @property
    def peer(self):
        return str(self._stream.socket.getpeername())

    def __init__(self, stream: trio.SocketStream):
        super().__init__()
        self._mutex = trio.Lock()
        self._stream = stream

    # the message format is <payload length>:4 <payload>
    # in other words, the length is encoded in the first 4 bytes
    # this is enough to support
    # - 256 ** 4 - 1 bytes (4 GiB) per message
    async def send(self, message: bytes):
        """Send a single message"""
        assert len(message) < 256 ** 4 - 1, 'message too large for %s' % self
        async with self._mutex:
            try:
                await self._stream.send_all(
                    len(message).to_bytes(4, byteorder='big') + message
                )
            except trio.BrokenResourceError:
                raise StreamBroken
            except trio.ClosedResourceError:
                raise StreamClosed

    async def read(self) -> bytes:
        """Read an entire ``message`` at once"""
        length = int.from_bytes(
            await self._receive_exactly(4),
            byteorder='big',
        )
        return await self._receive_exactly(length)

    async def _receive_exactly(self, byte_count: int) -> bytes:
        """Read exactly ``byte_count`` bytes"""
        message = b''
        async with self._mutex:
            try:
                async for chunk in async_iter(
                    lambda: self._stream.receive_some(byte_count - len(message)), b''
                ):
                    message += chunk
            except trio.BrokenResourceError:
                raise StreamBroken
            except trio.ClosedResourceError:
                raise StreamClosed
        if len(message) < byte_count:
            await self.aclose()
            raise StreamClosed
        return message

    async def aclose(self):
        await self._stream.aclose()

    def __str__(self):
        _socket = self._stream.socket
        return '%s<%s -> %s>' % (
            self.__class__.__name__, _socket.getsockname(), _socket.getpeername()
        )

    def __repr__(self):
        _socket = self._stream.socket
        return '<%s local=%s, remote=%s>' % (
            self.__class__.__name__, _socket.getsockname(), _socket.getpeername()
        )
