"""
Python version compatibility
"""
import typing

try:
    from contextlib import AbstractAsyncContextManager
except ImportError:
    AbstractAsyncContextManager = object

try:
    AsyncContextManager = typing.AsyncContextManager
except AttributeError:
    import abc
    import collections.abc
    from typing import Generic, TypeVar

    T_co = TypeVar('T_co', covariant=True)

    class AsyncContextManager(Generic[T_co], AbstractAsyncContextManager):
        """An abstract base class for asynchronous context managers."""
        async def __aenter__(self):
            """Return `self` upon entering the runtime context."""
            return self

        @abc.abstractmethod
        async def __aexit__(self, exc_type, exc_value, traceback):
            """Raise any exception triggered within the runtime context."""
            return None

        @classmethod
        def __subclasshook__(cls, C):
            if cls is AbstractAsyncContextManager:
                return collections.abc._check_methods(
                    C, "__aenter__", "__aexit__"
                )
            return NotImplemented

    typing.AsyncContextManager = AsyncContextManager
