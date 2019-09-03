from typing import AsyncIterator, Awaitable, TypeVar, Optional, Callable,\
    Iterable, Union, AsyncContextManager
from trio.abc import SendChannel
import functools

import trio
from async_generator import aclosing


T = TypeVar('T')


def acloseable(call: Callable[..., T]) -> Callable[..., AsyncContextManager[T]]:
    """Decorate a function to wrap the result in a ``.aclose`` async context manager"""
    @functools.wraps(call)
    def inner(*args, **kwargs) -> AsyncContextManager[T]:
        return aclosing(call(*args, **kwargs))  # type: AsyncContextManager[T]
    return inner


class AwaitableBool:
    """
    Wrapper around a :py:class:`trio.Event` providing an async boolean interface

    This type can be synchronously evaluated in a boolean context,
    such as ``bool(awaitable_bool)`` or ``if (awaitable_bool):``.
    In addition, a ``trio`` coroutine can delay until the event is set,
    such as ``await awaitable_bool``.
    """
    def __init__(self, event: trio.Event):
        self._event = event

    def __bool__(self):
        return self._event.is_set()

    def __await__(self):
        yield from self._event.wait().__await__()


def default(value: Optional[T], _or: T) -> T:
    """Given an optional ``value``, return the value if set or a default"""
    return value if value is not None else _or


class AsyncIterSentinel:
    """Sentinel for async iteration"""
    def __repr__(self):
        return self.__class__.__name__


def async_iter(
        *awaitable_calls: Callable[[], Awaitable[T]],
        sentinel: Union[T, AsyncIterSentinel] = AsyncIterSentinel(),
) -> AsyncIterator[T]:
    """
    Provide an AsyncIterator that repeatedly calls and awaits ``awaitable_calls``

    :param awaitable_calls: producer of values that the AsyncIterator provides
    :param sentinel: optional indicator for end of iteration
    :return: AsyncIterator over all values produced by ``awaitable_calls``

    In its simplest form with just one ``awaitable_calls``, this is equivalent
    to repeatedly calling, awaiting and yielding from its argument:

    .. code:: python3

        async def async_iter(awaitable_call):
            while True:
                yield await awaitable_call()

    With multiple ``awaitable_calls``, their results are merged in-order
    of availability. If ``sentinel`` is provided, any ``awaitable_calls``
    whose result equals ``sentinel`` is considered exhausted; once all
    ``awaitable_calls`` are exhausted, the ``async_iter`` is exhausted
    as well.
    """
    if len(awaitable_calls) == 1:
        return awaitable_call_iterator(awaitable_calls[0], sentinel)
    elif len(awaitable_calls) == 0:
        raise TypeError('async_iter expected at least 1 arguments, got 0')
    else:
        return multi_awaitable_call_iterator(
            awaitable_calls,
            sentinel
        )


async def awaitable_call_iterator(
        awaitable_call: Callable[[], Awaitable[T]],
        sentinel: Union[T, AsyncIterSentinel],
):
    if isinstance(sentinel, AsyncIterSentinel):
        while True:
            yield await awaitable_call()
    else:
        value = await awaitable_call()
        while value != sentinel:
            yield value
            value = await awaitable_call()


async def multi_awaitable_call_iterator(
        awaitable_calls: Iterable[Callable[[], Awaitable[T]]],
        sentinel: Union[T, AsyncIterSentinel],
):
    async with trio.open_nursery() as nursery:
        send_channel, receive_channel = trio.open_memory_channel(0)
        async with send_channel, receive_channel:
            for awaitable_call in awaitable_calls:
                nursery.start_soon(_multi_aci_task(
                    awaitable_call=awaitable_call,
                    channel=send_channel.clone(),
                    sentinel=sentinel,
                ))
            async for value in receive_channel:
                yield value


async def _multi_aci_task(
        awaitable_call: Callable[[], Awaitable[T]],
        channel: SendChannel,
        sentinel: Union[T, AsyncIterSentinel],
):
    if isinstance(sentinel, AsyncIterSentinel):
        while True:
            await channel.send(await awaitable_call())
    else:
        value = await awaitable_call()
        while value != sentinel:
            await channel.send(value)
            value = await awaitable_call()
    await channel.aclose()
