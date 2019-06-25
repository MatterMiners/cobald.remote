cobald.remote - Remote pools for COBalD
=======================================

This is a *draft* for integrating remote processes as COBalD Pools.

Pool Connections
----------------

Connect to an individual remote controller, or one or several remote pools.
For example, locally in a multi-process environment or distributed in a cluster.

The idea is to have "template" objects that specify the connection,
e.g. ``TCP(host='127.0.0.1', port=1337)``.
These generic objects allow to *create specific connections* for either
one controller, one pool or many pools.

This could use `Template.type` as for the ``Controller.s`` or a ``Type(Template)``.
We could also use just ``>> Pool``, ``Controller >>`` and ``for pool in pools:``.

.. code:: python3

    # constructor
    Controller(TCP('cobald_host', 12781).pool)
    TCP('cobald_host', 12781).controller(pool)

    # operator
    controller >> TCP('cobald_host', 12781)
    TCP('cobald_host', 12781) >> pool

    for pool in TCP('cobald_host', 12781):
        self._children.add(pool)

Connection Types
----------------

Possible variants for connections:

* Protocol: JSON, binary (BSON)
* Transport: TCP, IPC/Pipe, SSH?
* Security: SSL/TCP, HMAC/*, SSH/SSH

We basically always need Protocol + Connection, but may want one/multiple Security inserts.
The current Transport architecture and usage allows adding arbitrary wrappers using binary protocols.
So we could have Security as something like ``JSON(HMAC(SSL(TCP(...))))``:

.. code::

    from cobald.remote import JSON, SSL, TCP

    # operator based binding/pipes
    controller >> JSON >> SSL >> TCP('cobald_host', 12781)
    controller >> JSON + SSL + TCP('cobald_host', 12781)

    # parameter based factory
    controller >> TCP('cobald_host', 12781, protocol=JSON, auth=SSL)

    # explicit type wrappers
    controller >> JSON(SSL(TCP('cobald_host', 12781)))
