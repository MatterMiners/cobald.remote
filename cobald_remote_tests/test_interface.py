import pytest

from cobald.interfaces import Controller

from cobald.remote._interface.api import Protocol
from cobald.remote._protocol.json import JSON
from cobald.remote._protocol.binary import Bin
from cobald.remote._transport.loopback import MemoryTransport

from .mock import MockPool, MockController, accept_services
from .utility import poll


@pytest.fixture(params=(JSON, Bin))
def protocol(request):
    return request.param(MemoryTransport('test_json'))


def test_binding(protocol: Protocol):
    control_side = MockController.s() >> protocol.pool()  # type: MockController
    pool_side = protocol >> MockPool()
    assert isinstance(control_side, MockController)
    assert isinstance(pool_side, Controller)
    with accept_services(name='test_binding'):
        control_side.set_demand(2)
        assert poll(lambda: pool_side.target.demand == 2, timeout=2),\
            "Remote pool demand must be set"
        control_side.set_demand(400000)
        assert poll(lambda: pool_side.target.demand == 400000, timeout=2),\
            "Remote pool demand must be set"
        control_side.set_demand(0)
        assert poll(lambda: pool_side.target.demand == 0, timeout=2),\
            "Remote pool demand must be set"
