from typing import AsyncIterator, Awaitable, TypeVar, Optional

T = TypeVar('T')


def async_iter(awaitable: Awaitable[T], sentinel: Optional[T] = None) -> AsyncIterator[T]:
    if sentinel is None:
        return AsyncIter(awaitable)
    else:
        return AsyncSentinelIter(awaitable, sentinel)


def maybe(value: Optional[T], default: T) -> T:
    return value if value is not None else default


class AsyncIter:
    """
    AsyncIterator repeatedly invoking ``awaitable``
    """
    __slots__ = ('_awaitable',)

    def __init__(self, awaitable: Awaitable):
        self._awaitable = awaitable

    async def __anext__(self):
        return await self._awaitable

    def __aiter__(self):
        return self


class AsyncSentinelIter:
    """
    AsyncIterator repeatedly invoking ``awaitable`` until ``sentinel`` is observed
    """
    __slots__ = ('_awaitable', '_sentinel')

    def __init__(self, awaitable: Awaitable, sentinel):
        self._awaitable = awaitable
        self._sentinel = sentinel

    async def __anext__(self):
        value = await self._awaitable
        if value == self._sentinel:
            raise StopAsyncIteration
        return value

    def __aiter__(self):
        return self
