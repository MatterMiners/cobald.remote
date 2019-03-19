from typing import Dict, List, Tuple
import weakref
import trio
import threading
import json

from cobald.interfaces import Pool
from cobald.daemon import service

from cobald.remote._abc import Transport, SlaveHandler, MasterHandler, PoolsHandler, RemotePool, Connector


async def send(transport: Transport, **message):
    await transport.put(json.dumps(message))


async def serve(transport: Transport, **commands):
    async for raw_message in transport:
        message = json.loads(raw_message)
        if not isinstance(message, dict):
            continue
        try:
            handler = commands[message['command']]
            await handler(**message)
        except (KeyError, TypeError, AssertionError):
            continue


@service(flavour=trio)
class JsonSlaveHandler(SlaveHandler):
    def __init__(self, connector: Connector, channel_name: str = 'remote.portal.json', publish_interval: float = 10):
        super().__init__(connector=connector, channel_name=channel_name)
        self._mutex = threading.Lock()
        self._max_identifier = 0
        self._pools = weakref.WeakValueDictionary()  # type: weakref.WeakValueDictionary[int, Pool]
        self._publish_interval = publish_interval

    def publish(self, pool: Pool):
        with self._mutex:
            pool_identifier = self._max_identifier = self._max_identifier + 1
            self._pools[pool_identifier] = pool

    async def run(self):
        while True:
            transport = await self.connector.connect()
            self._logger.info('connected %r', transport)
            with trio.open_nursery() as scope:
                scope.start_soon(self._publish_all, transport)
                scope.start_soon(serve, transport, demand=self._handle_demand)

    async def _publish_all(self, transport: Transport):
        while True:
            with self._mutex:
                pools = list(self._pools.items())  # type: List[Tuple[int, Pool]]
            pools = {identifier: {
                'demand': pool.demand, 'supply': pool.supply,
                'utilisation': pool.utilisation, 'allocation': pool.allocation
            } for identifier, pool in pools}
            self._logger.debug('publish %d pools to %s', len(pools), transport)
            await send(transport, command='publish', pools=pools)
            await trio.sleep(self._publish_interval)

    async def _handle_demand(self, command, identifier: int, demand: float):
        assert command == 'demand'
        assert isinstance(identifier, int)
        assert isinstance(demand, (float, int))
        try:
            self._pools[identifier].demand = demand
        except KeyError:
            pass


@service(flavour=trio)
class JsonMasterHandler(MasterHandler):
    def __init__(self, connector: Connector, channel_name: str = 'remote.portal.json'):
        super().__init__(connector=connector, channel_name=channel_name)
        self._portal = None  # type: trio.BlockingTrioPortal
        self._adopt_channels = None  # type: Tuple[trio.abc.SendChannel, trio.abc.ReceiveChannel]

    async def run(self):
        self._portal = trio.BlockingTrioPortal()
        adopt_add, adopt_get = self._adopt_channels = trio.open_memory_channel()
        with trio.open_nursery() as scope:
            async with self.connector as connections:
                for transport in connections:
                    self._logger.info('accepted %r', transport)
                    handler = JsonPoolsHandler(transport, adopt_add)
                    scope.start_soon(handler.serve)

    def adopt(self):
        return PoolsIterable(channel=self._adopt_channels[1], portal=self._portal)


class JsonPoolsHandler(PoolsHandler):
    def __init__(self, transport: Transport, adopt_queue: trio.abc.SendChannel):
        self.transport = transport
        self._adopt_queue = adopt_queue
        self._pools = weakref.WeakValueDictionary()  # type: weakref.WeakValueDictionary[int, Pool]

    async def serve(self):
        await serve(self.transport, publish=self._handle_publish)

    async def demand(self, pool: RemotePool, value: float):
        if self._pools.get(pool.identifier, None) is pool:
            await send(self.transport, command='demand', identifier=pool.identifier, demand=value)

    async def _handle_publish(self, command, pools: Dict[int, Dict[str, float]]):
        assert command == 'publish'
        assert isinstance(pools, dict)
        for identifier, pool_state in pools.items():
            assert isinstance(identifier, int)
            assert isinstance(pool_state, dict)
            try:
                pool = self._pools[identifier]
            except KeyError:
                pool = self._pools[identifier] = RemotePool(self, identifier)
                pool.update(**pool_state)
                await self._adopt_queue.send(pool)
            else:
                pool.update(**pool_state)


class PoolsIterable:
    def __init__(self, channel: trio.abc.ReceiveChannel, portal: trio.BlockingTrioPortal):
        self.channel = channel
        self.portal = portal

    def __iter__(self):
        while True:
            yield self.portal.run(self.channel.receive)

    def __aiter__(self):
        return self.channel.__aiter__()
