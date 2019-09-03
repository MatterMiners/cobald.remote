from abc import ABC, abstractmethod
from typing import AsyncContextManager, AsyncIterator, Iterator, Type, TypeVar

from async_generator import aclosing, asynccontextmanager
from cobald.interfaces import Pool, Controller, Partial

from .streams import MessageStream, CobaldStream
from .proxy import RemoteController, RemotePool, ConnectedPool


class Transport(ABC):
    """
    Definition for transporting messages
    """
    __slots__ = ()

    @abstractmethod
    async def __connect__(self) -> AsyncContextManager[MessageStream]:
        raise NotImplemented

    async def __accept_one__(self) -> 'AsyncContextManager[MessageStream]':
        async with aclosing(self.__accept__()) as connections:
            async for connection in connections:
                return connection

    @abstractmethod
    async def __accept__(self) -> AsyncIterator[AsyncContextManager[MessageStream]]:
        raise NotImplemented


class Protocol(ABC):
    __slots__ = ('transport',)

    def __init__(self, transport: Transport):
        self.transport = transport

    def pool(self) -> RemotePool:
        return RemotePool(protocol=self)

    def controller(self, target: Pool, interval: float = 1) -> RemoteController:
        return RemoteController(target=target, protocol=self, interval=interval)

    def __iter__(self) -> Iterator[ConnectedPool]:
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[ConnectedPool]:
        raise NotImplementedError

    def __rshift__(self, other: Pool) -> RemoteController:
        return Partial(RemoteController, protocol=self, __leaf__=False) >> other

    def __rrshift__(self, other: Partial[Controller]) -> Pool:
        return other >> self.pool()

    @abstractmethod
    async def __connect__(self) -> AsyncContextManager[CobaldStream]:
        raise NotImplemented

    async def __accept_one__(self) -> 'AsyncContextManager[CobaldStream]':
        async with aclosing(self.__accept__()) as connections:
            async for connection in connections:
                return connection

    @abstractmethod
    def __accept__(self) -> AsyncIterator[AsyncContextManager[CobaldStream]]:
        raise NotImplemented


CS = TypeVar('CS', bound=CobaldStream)


@asynccontextmanager
async def stream_manager(
        message_manager: AsyncContextManager[MessageStream], stream: Type[CS],
        *args, **kwargs,
) -> AsyncContextManager[CS]:
    async with message_manager as message_stream:
        cobald_stream = stream(message_stream, *args, **kwargs)
        yield cobald_stream
