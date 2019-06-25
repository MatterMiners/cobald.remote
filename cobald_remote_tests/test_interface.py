from cobald.interfaces import Controller

from cobald.remote._protocol.json import JSON
from cobald.remote._transport.loopback import MemoryTransport

from .mock import MockPool, MockController, accept_services
from .utility import poll


class TestPipeline:
    def test_binding(self):
        remote = JSON(MemoryTransport('test_binding'))
        control_side = MockController.s() >> remote.pool()  # type: MockController
        pool_side = remote >> MockPool()
        assert isinstance(control_side, MockController)
        assert isinstance(pool_side, Controller)
        with accept_services(name='test_binding'):
            control_side.set_demand(2)
            assert poll(lambda: pool_side.target.demand == 2),\
                "Remote pool demand must be set"
