from cobald.remote.utility import sync_aiter

from .mock import accept_services


def test_sync_aiter():
    async def aiter(iterable):
        for item in iterable:
            yield item

    with accept_services(name='test_sync_aiter'):
        items = [1, 2, 3, -100, 1338, 451]
        async_items = list(sync_aiter(aiter(items)))
        assert items == async_items
