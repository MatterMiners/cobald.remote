import logging
from typing import AsyncIterator, NamedTuple, AsyncIterable, Iterable, TypeVar
from abc import ABC, abstractmethod

from cobald.interfaces import Pool, Controller

from .utility import maybe


PoolStat = NamedTuple('PoolStat', [
    ('demand', float), ('supply', float), ('utilisation', float), ('allocation', float), ('epoch', float)
])

T = TypeVar('T')


class SyncAsyncIterable(AsyncIterable, Iterable):
    """An Iterable suitable for both sync and async iteration"""


class Connector(ABC):
    @abstractmethod
    async def accept(self) -> 'Transport':
        ...

    @abstractmethod
    async def connect(self) -> 'Transport':
        ...

    @abstractmethod
    async def __aenter__(self) -> AsyncIterator['Transport']:
        ...


class Transport(ABC):
    """
    Duplex connection for unshared reads and writes of binary messages

    Transports are bi-directional streams of binary messages.
    Messages are received in-order of sending.
    """
    __slots__ = ()

    @abstractmethod
    async def put(self, *messages: bytes):
        """Send all ``messages`` before returning"""
        ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[bytes]:
        ...


class RemotePool(Pool):
    __slots__ = ('_identifier', '_handler', '_demand', '_supply', '_utilisation', '_allocation')

    def __init__(self, handler: 'PoolsHandler', identifier: int):
        self._handler = handler
        self.identifier = identifier
        self._demand, self._supply, self._utilisation, self._allocation = 0, 0, 1, 1

    @property
    def supply(self):
        return self._supply

    @property
    def demand(self):
        return self._demand

    @demand.setter
    def demand(self, value):
        if value != self._demand:
            self._handler.demand(self, value)
            self._demand = value

    @property
    def utilisation(self):
        return self._utilisation

    @property
    def allocation(self):
        return self._allocation

    def update(self, demand=None, supply=None, utilisation=None, allocation=None):
        self._demand = maybe(demand, self._demand)
        self._supply = maybe(supply, self._supply)
        self._utilisation = maybe(utilisation, self._utilisation)
        self._allocation = maybe(allocation, self._allocation)


class RemoteController(Controller):
    def __init__(self, target, portal: 'SlaveHandler'):
        super().__init__(target)
        self.portal = portal
        portal.publish(target)


# TODO: These names are ugly, find better ones
class SlaveHandler(ABC):
    """Protocol used by concrete pools"""
    __slots__ = ('connector', '_logger')

    def __init__(self, connector: Connector, channel_name: str = 'remote.portal'):
        self.connector = connector
        self._logger = logging.getLogger('cobald.runtime.%s' % channel_name)

    @abstractmethod
    def publish(self, pool: Pool):
        """Publish information about an existing pool to the remote side"""
        ...


class MasterHandler(ABC):
    __slots__ = ('connector', '_logger')

    def __init__(self, connector: Connector, channel_name: str = 'remote.portal'):
        self.connector = connector
        self._logger = logging.getLogger('cobald.runtime.%s' % channel_name)

    @abstractmethod
    def adopt(self) -> SyncAsyncIterable[RemotePool]:
        """Adopt every :py:class:`~.Pool` that is published from the remote side"""
        ...


class PoolsHandler(ABC):
    @abstractmethod
    def demand(self, pool: Pool, value: float):
        """Set :py:attr:`~.Pool.demand` to ``value`` for a remote side pool"""
        ...
