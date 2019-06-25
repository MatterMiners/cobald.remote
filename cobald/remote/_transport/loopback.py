from typing import AsyncIterator, AsyncContextManager, Dict, Any, Tuple

import trio
from trio.abc import SendChannel, ReceiveChannel
from async_generator import asynccontextmanager

from .._interface.api import Transport
from .._interface.streams import MessageStream, StreamClosed, StreamBroken


class MemoryTransport(Transport):
    _listener = {}  # type: Dict[Any, MemoryTransport]
    _peer = {}  # type: Dict[Any, Tuple[SendChannel, ReceiveChannel]]

    def __init__(self, identifier: Any):
        self.identifier = identifier

    async def __connect__(self) -> 'AsyncContextManager[MessageStream]':
        return await self._open_connection()

    async def __accept__(self) -> 'AsyncIterator[AsyncContextManager[MessageStream]]':
        if self._listener.setdefault(self.identifier, self) is not self:
            raise RuntimeError(
                "%s for %s already has a listener" % (
                    self.__class__.__name__, self.identifier
                ))
        try:
            while True:
                yield await self._open_connection()
        finally:
            self._listener.pop(self.identifier)

    async def _open_connection(self) -> 'AsyncContextManager[MessageStream]':
        my_send, my_receive = trio.open_memory_channel(max_buffer_size=0)
        try:
            other_send, other_receive = self._peer.pop(self.identifier)
        except KeyError:
            self._peer[self.identifier] = my_send, my_receive
            other_receive = await my_receive.receive()
        else:
            await other_send.send(my_receive)
        return scope_stream(my_send, other_receive)


@asynccontextmanager
async def scope_stream(
            send: SendChannel[bytes],
            receive: ReceiveChannel[bytes]
) -> AsyncContextManager[MessageStream]:
    async with send, receive:
        memory_stream = MemoryStream(send, receive)
        # print('open', memory_stream)
        yield memory_stream
        # print('close', memory_stream)


class MemoryStream(MessageStream):
    @property
    def peer(self):
        return '<this process>'

    def __init__(
            self,
            send: SendChannel[bytes],
            receive: ReceiveChannel[bytes]
    ):
        super().__init__()
        self._send_channel = send
        self._receive_channel = receive

    async def send(self, message: bytes):
        """Send a single message"""
        try:
            await self._send_channel.send(message)
        except trio.BrokenResourceError:
            raise StreamBroken
        except trio.ClosedResourceError:
            raise StreamClosed

    async def read(self) -> bytes:
        """Read an entire ``message`` at once"""
        try:
            return await self._receive_channel.receive()  # type: ignore
        except trio.BrokenResourceError:
            raise StreamBroken
        except trio.ClosedResourceError:
            raise StreamClosed

    async def aclose(self):
        await self._send_channel.aclose()
        await self._receive_channel.aclose()
