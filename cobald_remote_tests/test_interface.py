import pytest

from cobald.interfaces import Controller

from cobald.remote._interface.api import Protocol
from cobald.remote._protocol.json import JSON
from cobald.remote._protocol.binary import Bin
from cobald.remote._transport.loopback import MemoryTransport

from .mock import MockPool, MockController, accept_services
from .utility import poll


connections = 0


@pytest.fixture(params=(JSON, Bin))
def protocol(request):
    global connections
    connections += 1
    return request.param(MemoryTransport(
        f'protocol[{request.param.__name__},{connections}]'
    ))


def test_binding(protocol: Protocol):
    """Connect via ``>>`` to get lazy sync proxy"""
    control_side = MockController.s() >> protocol.pool()  # type: MockController
    pool_side = protocol >> MockPool()
    assert isinstance(control_side, MockController)
    assert isinstance(pool_side, Controller)
    with accept_services(name='test_binding'):
        assert control_side.target.peer is not None
        assert pool_side.peer is not None
        control_side.demand = 2
        assert poll(lambda: pool_side.target.demand == 2, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 2)
        control_side.demand = 400000
        assert poll(lambda: pool_side.target.demand == 400000, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 400000)
        control_side.demand = 0
        assert poll(lambda: pool_side.target.demand == 0, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 0)
        assert control_side.supply == pool_side.target.supply
        assert control_side.allocation == pool_side.target.allocation
        assert control_side.utilisation == pool_side.target.utilisation


def test_pool_iter(protocol: Protocol):
    """Connect directly to get bound async proxy"""
    pool_side = protocol >> MockPool()
    assert isinstance(pool_side, Controller)
    with accept_services(name='test_binding'):
        control_side = next(iter(protocol))
        assert control_side.peer is not None
        assert pool_side.peer is not None
        control_side.demand = 2
        assert poll(lambda: pool_side.target.demand == 2, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 2)
        control_side.demand = 400000
        assert poll(lambda: pool_side.target.demand == 400000, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 400000)
        control_side.demand = 0
        assert poll(lambda: pool_side.target.demand == 0, timeout=2),\
            "Remote pool demand must be set"
        assert poll(lambda: control_side.demand == 0)
        assert control_side.supply == pool_side.target.supply
        assert control_side.allocation == pool_side.target.allocation
        assert control_side.utilisation == pool_side.target.utilisation
