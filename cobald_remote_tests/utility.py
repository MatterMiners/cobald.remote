import time
from typing import Callable


def poll(what: Callable[[], bool], timeout: float = 1, interval: float = None):
    interval = interval if interval is not None else timeout / 100
    end_time = time.time() + timeout
    while time.time() < end_time:
        if what():
            return True
        time.sleep(interval)
    return False
