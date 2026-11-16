"""Small example of one OSC multicast sender and two receivers.

Run it on a host where IPv4 multicast works on loopback::

    python examples/multicast_group.py --interface 127.0.0.1

Both receivers join the same group and port and each receives every message
exactly once.  Press Ctrl+C to leave the group and close the sockets.
"""

import argparse
import threading
import time

from pythonosc import osc_server, udp_client
from pythonosc.dispatcher import Dispatcher


def make_dispatcher(name):
    dispatcher = Dispatcher()

    def handler(address, *args):
        print(f"[{name}] {address}: {args}")

    dispatcher.map("/light/*", handler)
    return dispatcher


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="239.255.42.99", help="IPv4 group")
    parser.add_argument("--port", type=int, default=9000, help="UDP port")
    parser.add_argument(
        "--interface",
        default="0.0.0.0",
        help="Local IPv4 address to join/send on ('0.0.0.0' = OS choice)",
    )
    parser.add_argument("--ttl", type=int, default=1, help="Multicast TTL")
    args = parser.parse_args()

    servers = [
        osc_server.MulticastOSCUDPServer(
            (args.group, args.port), make_dispatcher(name), interface=args.interface
        )
        for name in ("receiver-a", "receiver-b")
    ]
    threads = [
        threading.Thread(target=server.serve_forever, daemon=True) for server in servers
    ]
    try:
        for thread in threads:
            thread.start()

        client = udp_client.SimpleUDPClient(
            args.group,
            args.port,
            multicast_interface=args.interface,
            multicast_ttl=args.ttl,
            multicast_loop=True,
        )
        try:
            for level in range(1, 4):
                client.send_message("/light/1", level)
                time.sleep(0.2)
        finally:
            client.close()
    except KeyboardInterrupt:
        pass
    finally:
        # Leaves the groups and closes the sockets, also when serve_forever
        # never ran.
        for server in servers:
            server.shutdown()
        print("left group and closed sockets")
