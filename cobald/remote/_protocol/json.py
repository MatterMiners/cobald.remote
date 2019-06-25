import json

from .._interface.api import Protocol, stream_manager
from .._interface.streams import CobaldStream, MessageStream
from ..utility import acloseable, aclosing


class JSONStream(CobaldStream):
    async def demand(self, amount):
        await self.stream.send(
            json.dumps(
                ['d', amount]
            ).encode(
                'utf-8', errors='surrogateescape'
            )
        )

    @acloseable
    async def receive(self):
        async for message in self.stream:  # type: bytes
            mtype, demand = json.loads(
                message.decode('utf-8', errors='surrogateescape')
            )
            assert mtype == 'd'
            yield demand

    async def publish(self, supply, demand, utilisation, allocation):
        await self.stream.send(
            json.dumps(
                ['p', supply, demand, utilisation, allocation]
            ).encode(
                'utf-8', errors='surrogateescape'
            )
        )

    @acloseable
    async def subscribe(self):
        async for message in self.stream:  # type: bytes
            mtype, *features = json.loads(
                message.decode('utf-8', errors='surrogateescape')
            )
            assert mtype == 'p' and len(features) == 4
            yield tuple(features)


class JSON(Protocol):
    async def __connect__(self):
        message_stream = await self.transport.__connect__()
        return stream_manager(message_stream, JSONStream)

    async def __accept__(self):
        async with aclosing(self.transport.__accept__()) as accept:
            async for message_stream in accept:
                yield stream_manager(message_stream, JSONStream)
