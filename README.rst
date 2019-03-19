cobald.remote - Remote pools for COBalD
=======================================

This is a *draft* for integrating remote processes as COBalD Pools.

Pool Connections
----------------

Connect to an individual remote pool.
For example, in a multi-process environment.

.. note:: Use `Template.something` as for the ``Controller.s`` or a ``Type(Template)``?
          We should also be able to get away with ``>> Pool``, ``Controller >>`` and ``for pool in pools:``.

.. code:: python

    controller >> TCP('cobald_host', 12781).pool
    controller >> Pool(TCP('cobald_host', 12781))

    TCP('cobald_host', 12781).controller >> pool
    Controller(TCP('cobald_host', 12781)) >> pool

Accept any remote pool connecting.
For example, connecting with resources from a composite factory.

.. code:: python

    # inside a composite-factory-remote pool
    for pool in Pools(TCP('cobald_host', 12781)):
        self._children.add(pool)

Connection Types
----------------

Possible variants for connections:

* Protocol: JSON, binary (BSON)
* Transport: TCP, IPC/Pipe, SSH?
* Security: SSL/TCP, HMAC/*, SSH/SSH

We basically always need Protocol + Connection, but may want one/multiple Security inserts.
The current Transport architecture and usage allows adding arbitrary binary wrappers.
So we could have Sec as something like ``JSON(HMAC(SSL(TCP(...))))``:

.. code::

    from cobald.remote import JSON, SSL, TCP

    # operator based binding/pipes
    controller >> JSON + SSL + TCP('cobald_host', 12781)

    # parameter based factory
    controller >> TCP('cobald_host', 12781, protocol=JSON, auth=SSL)

    # explicit wrappers
    controller >> JSON(SSL(TCP('cobald_host', 12781)))
