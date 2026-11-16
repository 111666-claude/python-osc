Server
=========

The server receives OSC Messages from connected clients and invoked the appropriate callback functions with the dispatcher. There are several server types available.
Server implementations are available for both UDP and TCP protocols.

Blocking Server
-----------------

The blocking server type is the simplest of them all. Once it starts to serve, it blocks the program execution forever and remains idle inbetween handling requests. This type is good enough if your application is very simple and only has to react to OSC messages coming in and nothing else.

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    from pythonosc.osc_server import BlockingOSCUDPServer


    def print_handler(address, *args):
        print(f"{address}: {args}")


    def default_handler(address, *args):
        print(f"DEFAULT {address}: {args}")


    dispatcher = Dispatcher()
    dispatcher.map("/something/*", print_handler)
    dispatcher.set_default_handler(default_handler)

    ip = "127.0.0.1"
    port = 1337

    server = BlockingOSCUDPServer((ip, port), dispatcher)
    server.serve_forever()  # Blocks forever


Threading Server
------------------

Each incoming packet will be handled in it's own thread. This also blocks further program execution, but allows concurrent handling of multiple incoming messages. Otherwise usage is identical to blocking type. Use for lightweight message handlers.


Forking Server
-----------------

The process is forked every time a packet is coming in. Also blocks program execution forever. Use for heavyweight message handlers.


Multicast Server (IPv4)
------------------------

``MulticastOSCUDPServer`` receives one OSC stream on several machines or
processes at once: every receiver joins the same IPv4 multicast group and port
and is handed a copy of each datagram by the network, decoded by the usual
:class:`~pythonosc.dispatcher.Dispatcher`.

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    from pythonosc.osc_server import MulticastOSCUDPServer
    import threading

    def light_handler(address, *args):
        print(f"{address}: {args}")

    dispatcher = Dispatcher()
    dispatcher.map("/light/*", light_handler)

    group = "239.255.0.1"
    port = 9000
    interface = "192.168.1.20"  # local address of the NIC to receive on

    server = MulticastOSCUDPServer(
        (group, port), dispatcher, interface=interface
    )
    # Never binding the group directly is what allows several receivers:
    # the socket listens on ("", port) and joins the group on `interface`.

    thread = threading.Thread(target=server.serve_forever)

    try:
        thread.start()
        # ... do work ...
    finally:
        # Stops serving if it was started, leaves every group and closes the
        # socket.  Safe even if serve_forever() never ran.
        server.shutdown()

A server may also join several groups, for example one per fixture, and leave
them individually:

.. code-block:: python

    server.join("239.255.0.2", interface)
    server.leave("239.255.0.2", interface)
    server.memberships   # frozenset of (group, interface) pairs

``interface`` is the local IPv4 address of the interface to join on;
``"0.0.0.0"`` (the default) lets the operating system choose.  Only IPv4
multicast is supported.

Receivers on different hosts need the same multicast group and port; on one
host, several ``MulticastOSCUDPServer`` instances can listen on the same port.
The server sets ``SO_REUSEADDR``, the ``IP_MULTICAST_ALL=0`` option on Linux
(so a receiver only gets groups it joined itself), and always leaves the group
and closes the socket in :meth:`~MulticastOSCUDPServer.shutdown` /
:meth:`~MulticastOSCUDPServer.server_close`, even when leaving fails.

.. note::

   ``shutdown`` replaces ``socketserver.BaseServer.shutdown`` so that calling
   it before ``serve_forever`` (for example from the ``finally`` block of a
   ``with`` statement whose body raised) cleans up immediately rather than
   deadlocking.  Both ``shutdown`` and ``server_close`` are idempotent.

Platform notes: setting the receiving socket to only deliver joined groups
uses ``IP_MULTICAST_ALL`` on Linux, which CPython does not expose as a
constant (its value, 49, is used directly); on macOS/BSD the option does not
exist and the platform already filters per socket.  On Windows several sockets
binding the same multicast port is controlled by the platform policy; if
``MulticastBindError`` is raised, use different ports or a single receiving
process.

Errors are explicit, with typed exceptions in
:mod:`pythonosc.osc_multicast` that also remain ``OSError``/``ValueError``
instances where that is meaningful:

===============================  ==============================================
Exception                        Raised when
===============================  ==============================================
``InvalidMulticastGroupError``   address is not a usable IPv4 group (unicast,
                                 all-hosts, reserved 224.0.0.0/24, malformed)
``MulticastInterfaceError``      interface is not a local IPv4 address or the
                                 OS cannot use it
``MulticastBindError``           the local port cannot be bound
``MulticastMembershipError``     the OS rejects a join/leave
``MulticastAlreadyJoinedError``  joining a group already joined on this socket
``MulticastNotJoinedError``      leaving a group that is not joined
===============================  ==============================================


Async Server
-------------

This server type takes advantage of the asyncio functionality of python, and allows truly non-blocking parallel execution of both your main loop and the server loop. You can use it in two ways, exclusively and concurrently. In the concurrent mode other tasks (like a main loop) can run in parallel to the server, meaning that the server doesn't block further program execution. In exclusive mode the server task is the only task that is started.

Concurrent Mode
^^^^^^^^^^^^^^^^^

Use this mode if you have a main program loop that needs to run without being blocked by the server. The below example runs ``init_main()`` once, which creates the serve endpoint and adds it to the asyncio event loop. The transport object is returned, which is required later to clean up the endpoint and release the socket. Afterwards we start the main loop with ``await loop()``. The example loop runs 10 times and sleeps for a second on every iteration. During the sleep the program execution is handed back to the event loop which gives the serve endpoint a chance to handle incoming OSC messages. Your loop needs to at least do an ``await asyncio.sleep(0)`` every iteration, otherwise your main loop will never release program control back to the event loop.

.. code-block:: python

    from pythonosc.osc_server import AsyncIOOSCUDPServer
    from pythonosc.dispatcher import Dispatcher
    import asyncio


    def filter_handler(address, *args):
        print(f"{address}: {args}")


    dispatcher = Dispatcher()
    dispatcher.map("/filter", filter_handler)

    ip = "127.0.0.1"
    port = 1337


    async def loop():
        """Example main loop that only runs for 10 iterations before finishing"""
        for i in range(10):
            print(f"Loop {i}")
            await asyncio.sleep(1)


    async def init_main():
        server = AsyncIOOSCUDPServer((ip, port), dispatcher, asyncio.get_event_loop())
        transport, protocol = await server.create_serve_endpoint()  # Create datagram endpoint and start serving

        await loop()  # Enter main loop of program

        transport.close()  # Clean up serve endpoint


    asyncio.run(init_main())


Exclusive Mode
^^^^^^^^^^^^^^^^^

This mode comes without a main loop. You only have the OSC server running in the event loop initially. You could of course use an OSC message to start a main loop from within a handler.

.. code-block:: python

    from pythonosc.osc_server import AsyncIOOSCUDPServer
    from pythonosc.dispatcher import Dispatcher
    import asyncio


    def filter_handler(address, *args):
        print(f"{address}: {args}")


    dispatcher = Dispatcher()
    dispatcher.map("/filter", filter_handler)

    ip = "127.0.0.1"
    port = 1337

    server = AsyncIOOSCUDPServer((ip, port), dispatcher, asyncio.get_event_loop())
    server.serve()


Server Module Documentation
------------------------------

.. automodule:: pythonosc.osc_server
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.osc_multicast
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.osc_tcp_server
  :special-members:
  :members:
  :exclude-members: __weakref__
