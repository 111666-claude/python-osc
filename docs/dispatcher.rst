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


Default Handler
-----------------

It is possible to specify a handler callback function that is called for every unmatched address:

.. code-block:: python

    disp.set_default_handler(some_handler_function)

This is extremely useful if you quickly need to find out what addresses an undocumented device is transmitting on or for building a learning function for some controls.
The handler must have the same signature as map callbacks:

.. code-block:: python

    def some_callback(address: str, *osc_arguments: ArgValue) -> None: ...


Bounded Scheduling of Future Timetags
---------------------------------------

The default ``strict_timing`` behaviour sleeps on the receive path until a future
timetag is reached.  A device that bursts thousands of future-dated messages (a
lighting console uploading a cue list, for example) would therefore block
processing or, with the threading server, create an unbounded number of sleeping
threads.

Pass a bounded scheduler to the dispatcher to instead accept every message
immediately and invoke its callbacks later, when the timetag becomes due:

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    from pythonosc.scheduling import (
        ThreadEventScheduler,       # for the synchronous servers
        AsyncIOEventScheduler,      # for the asyncio servers
        OverflowPolicy,
        ClosePolicy,
    )

    scheduler = ThreadEventScheduler(
        capacity=10000,                          # bounded pending queue
        overflow_policy=OverflowPolicy.REJECT,  # what to do when full
        close_policy=ClosePolicy.CANCEL_PENDING,
    )
    disp = Dispatcher(scheduler=scheduler)

Properties of the scheduled mode:

* The receive path never waits on a timetag; a single scheduler thread plus a
  bounded worker pool run the callbacks, so slow callbacks neither block
  reception nor grow the thread count.  The asyncio scheduler runs everything on
  the event loop and creates no threads.
* Messages whose timetag is already in the past and IMMEDIATELY bundles still
  fire at once; nested-bundle timetags and the original ordering of messages
  sharing a timetag are preserved.
* ``capacity`` bounds the pending queue.  On overflow you can ``REJECT`` the new
  event (``schedule`` returns ``None``), ``RAISE`` ``QueueFullError``,
  ``DROP_OLDEST`` or ``DROP_NEWEST``.
* On ``close``/``aclose`` pending events are either cancelled (default), flushed
  immediately (``FLUSH``) or allowed to run at their scheduled time
  (``WAIT_PENDING``).  The return value is the number of events left unexecuted.
* ``scheduler.stats()`` returns a ``SchedulerStats`` snapshot (pending, fired,
  cancelled, dropped, rejected, error counts) and an optional ``listener``
  callback observes every event lifecycle transition.
* Pending events are cancellable: keep the handle returned by ``schedule()`` and
  call ``handle.cancel()``, or cancel everything with
  ``disp.cancel_scheduled()``.
* The clock is injectable (``clock=``), which makes scheduling, cancellation and
  shutdown fully deterministic in tests.

When a scheduler is configured it takes precedence over ``strict_timing`` for
future timetags only; the original immediate and strict-timing modes are
unchanged when no scheduler is supplied.


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
