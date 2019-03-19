from typing import Optional, Type
from abc import ABC, abstractmethod

import trio
from cobald.interfaces import Pool, Partial
from cobald.daemon import service

from ._abc import SyncAsyncIterable, RemotePool, RemoteController, Connector, MasterHandler, SlaveHandler


@service(flavour=trio)
class PoolProxy(Pool):
    def __init__(self, master_handler: MasterHandler):
        self._master_handler = master_handler
        self._demand = 0
        self._remote_pool = None  # type: Optional[RemotePool]

    @property
    def demand(self):
        if self._remote_pool is not None:
            return self._remote_pool.demand
        return self._demand

    @demand.setter
    def demand(self, value):
        if self._remote_pool is not None:
            self._remote_pool.demand = value
        self._demand = value

    @property
    def supply(self):
        if self._remote_pool is not None:
            return self._remote_pool.supply
        return 0

    @property
    def utilisation(self):
        if self._remote_pool is not None:
            return self._remote_pool.utilisation
        return 1

    @property
    def allocation(self):
        if self._remote_pool is not None:
            return self._remote_pool.allocation
        return 1

    async def run(self):
        async for pool in self._master_handler.adopt():
            self._remote_pool = pool
            break
        self._master_handler = None


class Protocol(ABC):
    __slave_handler__ = None  # type: Type[SlaveHandler]
    __master_handler__ = None  # type: Type[MasterHandler]

    def __init__(self, connector: Connector):
        self.connector = connector

    @property
    def pool(self) -> PoolProxy:
        """Provide a proxy for a single remote Pool"""
        return PoolProxy(self.__master_handler__())

    @property
    def pools(self) -> SyncAsyncIterable[RemotePool]:
        handler = self.__master_handler__()
        return handler.adopt()

    @property
    def controller(self) -> Partial[RemoteController]:
        return RemoteController.s(portal=self.__slave_handler__())
