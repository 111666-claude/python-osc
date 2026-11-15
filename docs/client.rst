Client
========

The client allows you to connect and exchange messages with an OSC server.
Client classes are available for UDP and TCP protocols.
The base client class ``send`` method expects an :class:`OSCMessage` object, which is then sent out over TCP or UDP.
Additionally, a simple client class exists that constructs the :class:`OSCMessage` object for you.

See the examples folder for more use cases.

Examples
---------

.. code-block:: python

    from pythonosc.udp_client import SimpleUDPClient

    ip = "127.0.0.1"
    port = 1337

    client = SimpleUDPClient(ip, port)  # Create client

    client.send_message("/some/address", 123)   # Send float message
    client.send_message("/some/address", [1, 2., "hello"])  # Send message with int, float and string

    # Alternatively, use a context manager to automatically close the socket
    with SimpleUDPClient(ip, port) as client:
        client.send_message("/some/address", 123)


.. code-block:: python

    from pythonosc.tcp_client import SimpleTCPClient

    ip = "127.0.0.1"
    port = 1337

    client = SimpleTCPClient(ip, port)  # Create client

    client.send_message("/some/address", 123)   # Send float message
    client.send_message("/some/address", [1, 2., "hello"])  # Send message with int, float and string

Bounded async TCP send queue
-----------------------------

When many coroutines send at once to a slow TCP receiver, use
``QueuedAsyncTCPClient`` (or the ``QueuedAsyncSimpleTCPClient`` convenience
subclass). A single background writer task drains a bounded FIFO queue, so
frames always leave in enqueue order and are never interleaved or truncated.
The queue can be limited by message count and/or by pending bytes; when full,
``send`` either waits for capacity or raises immediately depending on the
configured policy. The returned ``bool`` tells whether that particular message
was written (``True``) or dropped by a discarding shutdown (``False``).

.. code-block:: python

    import asyncio
    from pythonosc.tcp_client import QueuedAsyncSimpleTCPClient
    from pythonosc.send_queue import SendQueueFullPolicy

    async def main():
        async with QueuedAsyncSimpleTCPClient(
            "127.0.0.1",
            1337,
            max_messages=100,          # at most 100 pending messages
            max_bytes=1_000_000,       # and at most 1 MiB of pending frames
            full_policy=SendQueueFullPolicy.ERROR,  # raise instead of waiting
        ) as client:
            sent = await client.send_message("/some/address", 123)
            assert sent is True

        # Or shut down immediately, dropping anything still queued:
        # await client.close(drain=False)

    asyncio.run(main())

Client Module Documentation
---------------------------------

.. automodule:: pythonosc.udp_client
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.tcp_client
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.send_queue
  :special-members:
  :members:
  :exclude-members: __weakref__
