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

Multicast (IPv4)
-----------------

Sending to a multicast group uses the same client class; pass the group as the
address and, optionally, the outgoing interface, TTL and loopback behavior.

.. code-block:: python

    from pythonosc.udp_client import SimpleUDPClient

    group = "239.255.0.1"
    port = 9000

    client = SimpleUDPClient(
        group,
        port,
        multicast_interface="192.168.1.20",  # outgoing NIC, "0.0.0.0" = OS choice
        multicast_ttl=1,                      # 0-255; 1 stays on the local subnet
        multicast_loop=True,                  # deliver to receivers on this host
    )
    client.send_message("/light/1", [255, 128, 0])

``multicast_interface``, ``multicast_ttl`` and ``multicast_loop`` are only
valid when the address is an IPv4 multicast group; with a unicast address they
raise ``osc_multicast.InvalidMulticastGroupError`` instead of being silently
ignored, and an unusable interface raises
``osc_multicast.MulticastInterfaceError``.

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
