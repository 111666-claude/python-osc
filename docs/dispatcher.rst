Dispatcher
============

The dispatcher maps OSC addresses to functions and calls the functions with the messages' arguments.
Function can also be mapped to wildcard addresses.


Example
---------

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    from pythonosc.osc_message_builder import ArgValue

    dispatcher = Dispatcher()


    def set_filter(address: str, *args: ArgValue) -> None:
        # We expect two float arguments
        if not len(args) == 2 or type(args[0]) is not float or type(args[1]) is not float:
            return

        # Check that address starts with filter
        if not address[:-1] == "/filter":  # Cut off the last character
            return

        value1 = args[0]
        value2 = args[1]
        filterno = address[-1]
        print(f"Setting filter {filterno} values: {value1}, {value2}")


    dispatcher.map("/filter*", set_filter)  # Map wildcard address to set_filter function

    # Set up server and client for testing
    from pythonosc.osc_server import BlockingOSCUDPServer
    from pythonosc.udp_client import SimpleUDPClient

    server = BlockingOSCUDPServer(("127.0.0.1", 1337), dispatcher)
    client = SimpleUDPClient("127.0.0.1", 1337)

    # Send message and receive exactly one message (blocking)
    client.send_message("/filter1", [1., 2.])
    server.handle_request()

    client.send_message("/filter8", [6., -2.])
    server.handle_request()


Mapping
---------

The dispatcher associates addresses with functions by storing them in a mapping.
An address can contains wildcards as defined in the OSC specifications.
Call the ``Dispatcher.map`` method with an address pattern and a handler callback function:

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    disp = Dispatcher()
    disp.map("/some/address*", some_printing_func)

This will for example print any OSC messages starting with ``/some/address``.

Additionally you can provide any amount of extra fixed argument that will always be passed before the OSC message arguments:

.. code-block:: python

    handler = disp.map("/some/other/address", some_printing_func, "This is a fixed arg", "and this is another fixed arg")

The handler callback signature must look like this (choose the form matching
whether fixed arguments were supplied):

.. code-block:: python

    from typing import Any, List
    from pythonosc.osc_message_builder import ArgValue

    def some_callback(address: str, *osc_arguments: ArgValue) -> None: ...
    def some_callback(address: str, fixed_argument: List[Any], *osc_arguments: ArgValue) -> None: ...

The annotation on ``*osc_arguments`` describes each received OSC argument;
inside the callback, ``osc_arguments`` is a tuple of those values. ``ArgValue``
includes scalar values, blobs, MIDI packets and OSC arrays. Fixed arguments
passed to ``map`` are instead collected into one separate ``List[Any]``.
You can also declare a fixed number of positional parameters when you know
which OSC arguments the address accepts.

The ``Dispatcher.map`` method returns a ``Handler`` object, which can be used to remove the mapping from the dispatcher.


Unmapping
-----------

A mapping can be undone with the ``Dispatcher.unmap`` method, which takes an address and ``Handler`` object as arguments.
For example, to unmap the mapping from the `Mapping`_ section:

.. code-block:: python

    disp.unmap("some/other/address", handler)

Alternatively the handler can be reconstructed from a function and optional fixed argument:

.. code-block:: python

    disp.unmap("some/other/address", some_printing_func, *some_fixed_args)

If the provided mapping doesn't exist, a ``ValueError`` is raised.


Rate Limiting
---------------

A high-frequency OSC source (for example a controller continuously streaming
knob values) can monopolize a dispatcher and starve messages on slower
addresses. To prevent that, a mapped address can be given its own rate limit
by passing ``rate`` (messages per second) and optionally ``burst`` (the
maximum number of messages that may be delivered at once) to
``Dispatcher.map``. Each rate limited address gets an independent token bucket
plus a bounded queue, so one busy address can never consume the budget of
another:

.. code-block:: python

    from pythonosc.rate_limit import OverflowPolicy

    # At most 30 knob messages per second, allowing an initial burst of 5.
    disp.map("/knob", knob_handler, rate=30, burst=5)

When a message arrives and no token is available it is queued (the queue holds
up to ``burst`` messages) and delivered later, in order, as tokens refill. Two
overflow strategies decide what happens when that queue is full:

* ``OverflowPolicy.DROP_OLDEST`` (the default): the oldest queued message is
  discarded so the newest value survives. Use this when only the latest value
  matters (typical for continuous controller positions).
* ``OverflowPolicy.REJECT_NEW``: the incoming message is dropped and the
  already queued messages are preserved.

.. code-block:: python

    disp.map(
        "/knob",
        knob_handler,
        rate=30,
        burst=5,
        overflow_policy=OverflowPolicy.REJECT_NEW,
    )

Per-address counters are available for monitoring:

.. code-block:: python

    stats = disp.rate_limit_stats("/knob")
    print(stats.delivered, stats.rejected, stats.dropped_old)

Messages inside bundles keep honoring their timetags: a future-dated message is
held until both its token and its timetag are due. An address's rate limit can
be changed or removed at runtime with ``Dispatcher.set_rate_limit`` (pass
``rate=None`` to remove it).

Rate limiting runs on the same clock as bundle timing. By default a background
scheduler thread (synchronous servers) or asyncio task (asynchronous servers)
releases queued messages without needing further traffic; both are stopped
deterministically by ``Dispatcher.shutdown`` (or the server's ``server_close`` /
``aclose``), optionally flushing the queues first. For deterministic tests a
virtual clock and manual draining can be injected through the ``Dispatcher``
constructor's ``clock``, ``sleep`` and ``auto_schedule=False`` arguments, and
queued messages can then be released explicitly with ``drain_pending`` /
``adrain_pending``.


Default Handler
-----------------

It is possible to specify a handler callback function that is called for every unmatched address:

.. code-block:: python

    disp.set_default_handler(some_handler_function)

This is extremely useful if you quickly need to find out what addresses an undocumented device is transmitting on or for building a learning function for some controls.
The handler must have the same signature as map callbacks:

.. code-block:: python

    def some_callback(address: str, *osc_arguments: ArgValue) -> None: ...


Handler Responses
-----------------

Handler functions can return responses back to the client, when running on a server, or to the
server when running as a client.  Handler functions should return one of:

* None
* An OSC address in string format
* A tuple containing a string OSC address and the associated arguments

If the handler function response is not None it will be encoded in an OSCMessage and sent to the
remote client or server.

Dispatcher Module Documentation
---------------------------------

.. automodule:: pythonosc.dispatcher
  :special-members:
  :members:
  :exclude-members: __weakref__
