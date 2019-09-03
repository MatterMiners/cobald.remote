import contextlib
import threading

from cobald.daemon.runners.service import ServiceRunner
from cobald.interfaces import Pool, Controller


class MockController(Controller):
    @property
    def demand(self):
        return self.target.demand

    @demand.setter
    def demand(self, value: float):
        self.target.demand = value

    @property
    def supply(self):
        return self.target.supply

    @property
    def utilisation(self):
        return self.target.utilisation

    @property
    def allocation(self):
        return self.target.allocation


class MockPool(Pool):
    """Pool allowing to set every attribute"""
    demand, supply, allocation, utilisation = 0, 0, 0, 0

    def __init__(self, demand=0, supply=0, allocation=0.5, utilisation=0.5):
        self.demand = demand
        self.supply = supply
        self.allocation = allocation
        self.utilisation = utilisation


@contextlib.contextmanager
def accept_services(payload: ServiceRunner = None, name=None):
    payload = payload if payload is not None else ServiceRunner(accept_delay=0.1)
    thread = threading.Thread(
        target=payload.accept, name=name or str(payload), daemon=True
    )
    thread.start()
    if not payload.running.wait(1):
        raise RuntimeError('%s failed to start' % payload)
    try:
        yield
    finally:
        payload.shutdown()
        thread.join()
