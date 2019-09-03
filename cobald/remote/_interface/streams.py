from abc import ABC, abstractmethod
from typing import AsyncIterator, AsyncContextManager, Tuple

from ..utility import async_iter


class StreamExpired(Exception):
    """A stream can no longer transmit data"""


class StreamBroken(StreamExpired):
    """A stream can no longer transmit data due to an error"""


class StreamClosed(StreamExpired):
    """A stream can no longer transmit data after being closed"""


class MessageStream(ABC):
    """
    Duplex connection for unshared reads and writes of byte messages

    Connections provide bi-directional streams of byte messages,
    which are received in-order of sending.


    .. code:: python3

        # initiate new connection with individual messages
        await stream.send('Hello')
        assert await stream.read() == 'World'

        #
        async for message in stream:
            if message == 'ping':
                await streams.send('pong')
    """
    __slots__ = ()

    @property
    @abstractmethod
    def peer(self) -> str:
        """Identifier of the remote peer"""

    @abstractmethod
    async def send(self, message: bytes):
        """Send an entire ``message`` at once"""

    @abstractmethod
    async def read(self) -> bytes:
        """Read an entire ``message`` at once"""

    @abstractmethod
    async def aclose(self):
        """Gracefully close the stream and notify its peer"""

    def __aiter__(self) -> AsyncIterator[bytes]:
        return async_iter(self.read)


class CobaldStream(ABC):
    """Stream that transmits information of the cobald pool model"""
    __slots__ = ('stream',)

    @property
    def peer(self) -> str:
        return self.stream.peer

    def __init__(self, stream: MessageStream):
        self.stream = stream

    @abstractmethod
    async def demand(self, amount: float):
        raise NotImplementedError

    @abstractmethod
    def receive(self) -> AsyncContextManager[AsyncIterator[float]]:
        raise NotImplementedError

    @abstractmethod
    def publish(
        self,
        supply: float, demand: float, utilisation: float, allocation: float
    ):
        raise NotImplementedError

    @abstractmethod
    def subscribe(
            self,
    ) -> AsyncContextManager[AsyncIterator[Tuple[float, float, float, float]]]:
        raise NotImplementedError

    async def aclose(self):
        """Gracefully close the stream and notify its peer"""
        await self.stream.aclose()
