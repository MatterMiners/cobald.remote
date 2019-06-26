from collections import deque
from typing import TYPE_CHECKING, Optional, AsyncContextManager
import time

import trio
from async_generator import aclosing
from cobald.daemon import service
from cobald.interfaces import Pool, Controller

from .streams import CobaldStream, StreamExpired, StreamClosed, StreamBroken
from ..utility import default, AwaitableBool

if TYPE_CHECKING:
    from .api import Protocol


class IntervalWindow:
    __slots__ = 'intervals', '_last_update'

    def __init__(self, size: int = 10):
        self.intervals = deque((0.1,), maxlen=size)
        self._last_update = time.time() - 0.1

    def update(self):
        """Add a new measurement point"""
        now = time.time()
        self.intervals.append(now - self._last_update)
        self._last_update = now

    async def __aiter__(self):
        while True:
            yield
            max_interval = max(self.intervals)
            avg_interval = sum(self.intervals) / len(self.intervals)
            delay = self._last_update + (
                    avg_interval + (max_interval - avg_interval) * 1.5
            ) - time.time()
            try:
                await trio.sleep(delay)
            except ValueError:  # missed time.time() by an instant
                await trio.sleep(-delay)


@service(flavour=trio)
class ConnectedPool(Pool):
    __slots__ = (
        '_stream', '_delays', '_closed', '_peer',
        '_supply', '_demand', '_allocation', '_utilisation',
    )

    @property
    def peer(self) -> str:
        """Identifier of the remote peer"""
        return default(self._peer, '<unconnected>')

    @property
    def supply(self):
        return self._supply

    @property
    def demand(self):
        return self._demand

    @demand.setter
    def demand(self, value):
        self._demand = value
        self._delays.update()

    @property
    def allocation(self):
        return self._allocation

    @property
    def utilisation(self):
        return self._utilisation

    @property
    def expired(self) -> AwaitableBool:
        """
        Whether this pool is no longer connected

        This is a :py:class:`~.AwaitableBool`, so it can be used both in a
        boolean expression for immediate evaluation, or in an ``await``
        expression to delay until expiration.

        .. code:: python3

            if not pool.expired:  # test if expired NOW
                print(pool, 'is still connected')
                await pool.expired  # wait until expired
                print(pool, 'is disconnected now')
        """
        return AwaitableBool(self._closed)

    def __init__(self, stream: AsyncContextManager[CobaldStream]):
        self._delays = IntervalWindow(size=10)
        self._demand, self._supply, self._utilisation, self._allocation = 0, 0, 1, 1
        self._stream = stream
        self._closed = trio.Event()
        self._peer = None  # type: Optional[str]

    async def run(self):
        try:
            with self._stream as stream:  # type: CobaldStream
                self._peer = stream.peer
                with trio.open_nursery() as nursery:
                    nursery.start_soon(self._publish_demand(), stream)
                    await self._receive_state(stream)
        except StreamExpired:
            self._closed.set()

    async def _publish_demand(self, stream: CobaldStream):
        async for _ in self._delays:
            await stream.demand(self._demand)

    async def _receive_state(self, stream: CobaldStream):
        async with stream.subscribe() as subscription:
            async for supply, demand, utilisation, allocation in subscription:
                self._supply = supply
                self._demand = demand
                self._utilisation = utilisation
                self._allocation = allocation


@service(flavour=trio)
class RemotePool(Pool):
    __slots__ = (
        '_protocol', '_delays', '_peer',
        '_supply', '_demand', '_allocation', '_utilisation'
    )

    @property
    def peer(self) -> str:
        """Identifier of the remote peer"""
        return default(self._peer, '<unconnected>')

    @property
    def supply(self):
        return self._supply

    @property
    def demand(self):
        return self._demand

    @demand.setter
    def demand(self, value):
        self._demand = value
        self._delays.update()

    @property
    def allocation(self):
        return self._allocation

    @property
    def utilisation(self):
        return self._utilisation

    def __init__(self, protocol: 'Protocol'):
        self._delays = IntervalWindow(size=10)
        self._demand, self._supply, self._utilisation, self._allocation = 0, 0, 1, 1
        self._protocol = protocol
        self._peer = None  # type: Optional[str]

    async def run(self):
        async with trio.open_nursery() as nursery,\
          aclosing(self._protocol.__accept__()) as accept:
            while True:
                with trio.fail_after(60*5):
                    connection = await accept.__anext__()
                async with connection as stream:  # type: CobaldStream
                    self._peer = stream.peer
                    nursery.start_soon(self._publish_demand, stream)
                    try:
                        await self._receive_state(stream)
                    except StreamClosed:
                        return True
                    except StreamBroken:
                        continue

    async def _publish_demand(self, stream: CobaldStream):
        try:
            async for _ in self._delays:
                await stream.demand(self._demand)
        except StreamExpired:
            pass

    async def _receive_state(self, stream: CobaldStream):
        async with stream.subscribe() as subscription:
            async for supply, demand, utilisation, allocation in subscription:
                self._supply = supply
                self._demand = demand
                self._utilisation = utilisation
                self._allocation = allocation


@service(flavour=trio)
class RemoteController(Controller):
    __slots__ = ('_protocol', '_peer', '_interval')

    @property
    def peer(self) -> str:
        """Identifier of the remote peer"""
        return default(self._peer, '<unconnected>')

    def __init__(self, target: Pool, protocol: 'Protocol', interval: float = 1):
        super().__init__(target)
        self._protocol = protocol
        self._peer = None  # type: Optional[str]
        self._interval = interval

    async def run(self):
        async with trio.open_nursery() as nursery:
            while True:
                with trio.fail_after(60*5):
                    connection = await self._protocol.__connect__()
                async with connection as stream:  # type: CobaldStream
                    self._peer = stream.peer
                    nursery.start_soon(self._publish, stream)
                    try:
                        await self._receive(stream)
                    except StreamClosed:
                        return True
                    except StreamBroken:
                        continue

    async def _publish_once(self, stream: CobaldStream):
        pool = self.target
        await stream.publish(
            supply=pool.supply, demand=pool.demand,
            utilisation=pool.utilisation, allocation=pool.allocation,
        )

    async def _receive(self, stream: CobaldStream):
        async with stream.receive() as demands:
            async for demand in demands:
                self.target.demand = demand
                await self._publish_once(stream)

    async def _publish(self, stream: CobaldStream):
        try:
            while True:
                await self._publish_once(stream)
                await trio.sleep(self._interval)
        except StreamExpired:
            pass
