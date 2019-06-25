import struct

from .._interface.api import Protocol, Transport, stream_manager
from .._interface.streams import CobaldStream, MessageStream
from ..utility import acloseable, aclosing


def _enforce_order(fmt: bytes) -> bytes:
    if fmt[0] == b'>':
        return fmt
    elif fmt[0] not in b'@=<!':
        return b'>' + fmt
    else:
        return b'>' + fmt[1:]


class BinaryStream(CobaldStream):
    __slots__ = ('_demand', '_publish')

    def __init__(self, stream: MessageStream, demand: bytes, publish: bytes):
        super().__init__(stream=stream)
        self._demand = struct.Struct(_enforce_order(demand))
        self._publish = struct.Struct(_enforce_order(publish))

    async def demand(self, amount):
        await self.stream.send(self._demand.pack(amount))

    @acloseable
    async def receive(self):
        async for message in self.stream:  # type: bytes
            demand, = self._demand.unpack(message)
            yield demand

    async def publish(self, supply, demand, utilisation, allocation):
        await self.stream.send(
            self._publish.pack(supply, demand, utilisation, allocation)
        )

    @acloseable
    async def subscribe(self):
        async for message in self.stream:  # type: bytes
            supply, demand, utilisation, allocation = self._publish.unpack(message)
            yield supply, demand, utilisation, allocation


class Bin(Protocol):
    __slots__ = ('_demand', '_publish')

    def __init__(self, transport: Transport, *, demand=b'q', publish=b'4e'):
        super().__init__(transport=transport)
        self._demand = demand
        self._publish = publish

    async def __connect__(self):
        message_stream = await self.transport.__connect__()
        return stream_manager(
            message_stream,
            BinaryStream, demand=self._demand, publish=self._publish
        )

    async def __accept__(self):
        async with aclosing(self.transport.__accept__()) as accept:
            async for message_stream in accept:
                yield stream_manager(
                    message_stream,
                    BinaryStream, demand=self._demand, publish=self._publish
                )
