Recording and Replay
====================

For post-show reviews and repeatable tuning sessions, python-osc can record the
raw UDP datagrams a server receives and replay them later through an ordinary
:class:`~pythonosc.udp_client.UDPClient`. Datagrams are stored byte for byte:
messages and nested bundles, including their timetag bytes, are never parsed or
rewritten, and normal dispatch continues unaffected while recording.

Recording
---------

Attach a :class:`~pythonosc.udp_recorder.DatagramRecorder` to any UDP server.
The recorder captures each accepted datagram together with its arrival order
and a relative timestamp taken from a monotonic clock.

.. code-block:: python

    from pythonosc.dispatcher import Dispatcher
    from pythonosc.osc_server import BlockingOSCUDPServer
    from pythonosc.udp_recorder import DatagramRecorder, OverflowPolicy

    dispatcher = Dispatcher()
    dispatcher.map("/filter", print)

    recorder = DatagramRecorder(
        max_packets=10000,
        overflow_policy=OverflowPolicy.DROP_OLDEST,
    )
    server = BlockingOSCUDPServer(("127.0.0.1", 5005), dispatcher, recorder=recorder)
    server.serve_forever()

The same ``recorder`` argument works with ``ThreadingOSCUDPServer`` and
``AsyncIOOSCUDPServer``. Recording happens on the server's main thread in
exact socket arrival order, before handlers run, so threaded handlers cannot
reorder the track. Recording failures are logged and never interrupt
dispatch.

Memory is bounded by ``max_packets``. When the track is full, the
:class:`~pythonosc.udp_recorder.OverflowPolicy` decides what happens:

* ``DROP_OLDEST`` (default) evicts the oldest datagram to make room.
* ``DROP_NEWEST`` discards the incoming datagram.
* ``RAISE`` raises :class:`~pythonosc.udp_recorder.CapacityOverflowError`.

Datagrams that are not valid OSC packets (and therefore would not be
dispatched either) are not recorded. If the injected clock goes backwards,
:class:`~pythonosc.udp_recorder.ClockRegressionError` is raised by the
recorder.

Replaying
---------

A :class:`~pythonosc.udp_recorder.DatagramPlayer` sends a recorded track back
out through an existing UDP client. The recorded intervals are divided by the
playback ``rate``: ``2.0`` replays twice as fast, ``0.5`` at half speed.

.. code-block:: python

    from pythonosc.udp_client import UDPClient
    from pythonosc.udp_recorder import DatagramPlayer

    recorder.stop()
    track = recorder.track
    if track.is_empty:
        print("nothing recorded")
    else:
        client = UDPClient("127.0.0.1", 5006)
        with DatagramPlayer(client, rate=2.0) as player:
            sent = player.play(track)
            print(f"replayed {sent} datagrams")
        client.close()

Playback runs on one dedicated thread that schedules each send lazily from
the recorded timestamps; there is no pre-built queue, so a slow receiver
cannot grow memory without bound. Call :meth:`~DatagramPlayer.cancel` to skip
every datagram that has not been sent yet, and
:meth:`~DatagramPlayer.close` to cancel and join the thread. Closing the
player (or using it as a context manager) leaves no background tasks behind.
An invalid rate (zero, negative, infinite or NaN) raises
:class:`~pythonosc.udp_recorder.InvalidRateError`.

Recorder/Player API
-------------------

.. automodule:: pythonosc.udp_recorder
  :special-members:
  :members:
  :exclude-members: __weakref__
