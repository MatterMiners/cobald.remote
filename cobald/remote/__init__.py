from ._abc import RemoteController, RemotePool
from ._api import PoolProxy, Protocol
from ._protocol.json import JsonMasterHandler as _JsonMasterHandler, JsonSlaveHandler as _JsonSlaveHandler
from ._transport.tcp import TcpConnector as TCP


class JSON(Protocol):
    __slave_handler__ = _JsonSlaveHandler
    __master_handler__ = _JsonMasterHandler
