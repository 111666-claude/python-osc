"""Record OSC UDP datagrams and replay them back over loopback.

Run the recorder, send some OSC traffic to its port (e.g. with
``examples/simple_client.py``), press Ctrl+C, and the captured session is
replayed to ``--replay-port`` at the requested rate.

The recorder stores raw datagram bytes, so messages and nested bundles
(including their timetag bytes) are replayed exactly as they arrived.
"""

import argparse

from pythonosc.dispatcher import Dispatcher
from pythonosc import osc_server
from pythonosc.udp_client import UDPClient
from pythonosc.udp_recorder import DatagramPlayer, DatagramRecorder, OverflowPolicy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="127.0.0.1", help="The ip to listen on")
    parser.add_argument(
        "--port", type=int, default=5005, help="The port to record on"
    )
    parser.add_argument(
        "--replay-port",
        type=int,
        default=5006,
        help="Datagrams are replayed to this port after recording stops",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Replay time multiplier (2.0 is twice as fast)",
    )
    parser.add_argument(
        "--capacity", type=int, default=10000, help="Maximum datagrams to keep"
    )
    args = parser.parse_args()

    dispatcher = Dispatcher()
    dispatcher.map("/*", print)
    recorder = DatagramRecorder(
        max_packets=args.capacity, overflow_policy=OverflowPolicy.DROP_OLDEST
    )
    server = osc_server.BlockingOSCUDPServer(
        (args.ip, args.port), dispatcher, recorder=recorder
    )

    print(f"Recording on {server.server_address}, press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        recorder.stop()

    track = recorder.track
    print(
        f"Recorded {len(track)} datagrams "
        f"({recorder.dropped_packets} dropped by overflow policy)"
    )
    if track.is_empty:
        print("Empty track: nothing to replay")
        return

    client = UDPClient(args.ip, args.replay_port)
    with DatagramPlayer(client, rate=args.rate) as player:
        print(f"Replaying to {args.ip}:{args.replay_port} at {args.rate}x")
        sent = player.play(track)
        print(
            f"Replayed {sent} datagrams "
            f"({player.cancelled_packets} cancelled)"
        )
    client.close()


if __name__ == "__main__":
    main()
