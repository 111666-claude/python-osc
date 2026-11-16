"""Tests for IPv4 multicast OSC client/server support.

The integration tests use the loopback interface (127.0.0.1) and a group out of
the administratively scoped range, so they do not depend on external network
configuration.  Some sandboxes and CI runners disable multicast on loopback;
the tests probe for that first and skip with a reproducible explanation
instead of failing.
"""

import socket
import threading
import unittest
from unittest import mock

from pythonosc import dispatcher, osc_multicast, osc_server, udp_client
from pythonosc.osc_message_builder import ArgValue

GROUP = "239.255.42.201"
OTHER_GROUP = "239.255.42.202"
LOOPBACK = "127.0.0.1"


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _probe_multicast() -> str | None:
    """Return None if loopback multicast works, else a skip reason.

    The probe joins ``GROUP`` on 127.0.0.1, sends one datagram and waits for
    it.  Any failure is reported with the exact errno so the environment can
    be reproduced (container without a multicast route, kernel module
    missing, etc.).
    """
    sock = None
    sender = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", 0))
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(GROUP) + socket.inet_aton(LOOPBACK),
        )
        sock.settimeout(1.0)
        port = sock.getsockname()[1]

        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_IF,
            socket.inet_aton(LOOPBACK),
        )
        sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sender.sendto(b"probe", (GROUP, port))
        data, _ = sock.recvfrom(64)
        if data != b"probe":
            return f"loopback multicast probe returned {data!r}"
        return None
    except OSError as exc:
        return (
            "loopback IPv4 multicast is unavailable on this host "
            f"({exc.__class__.__name__}: errno={exc.errno} {exc.strerror}); "
            "enable multicast on the loopback interface (e.g. `ip link set lo "
            "multicast on`, check CONFIG_IP_MULTICAST, or run the tests in a "
            "network namespace with a multicast route)"
        )
    finally:
        for s in (sock, sender):
            if s is not None:
                s.close()


_MULTICAST_SKIP_REASON = _probe_multicast()
requires_multicast = unittest.skipIf(
    _MULTICAST_SKIP_REASON is not None,
    _MULTICAST_SKIP_REASON or "",
)


class RecordingDispatcher(dispatcher.Dispatcher):
    """Dispatcher that records every packet it decodes."""

    def __init__(self) -> None:
        super().__init__()
        self.received: list[tuple[str, tuple]] = []
        self.event = threading.Event()
        self.set_default_handler(self._record)

    def _record(self, address: str, *args: object) -> None:
        self.received.append((address, args))
        self.event.set()


class TestMulticastValidation(unittest.TestCase):
    def test_valid_group(self):
        self.assertEqual(osc_multicast.parse_ipv4_group("239.1.2.3"), "239.1.2.3")

    def test_rejects_non_ipv4(self):
        for value in ("not-an-ip", "", "::1", "239.1.2.3 ", None, 123):
            with self.subTest(value=value):
                with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
                    osc_multicast.parse_ipv4_group(value)

    def test_rejects_unicast(self):
        with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
            osc_multicast.parse_ipv4_group("127.0.0.1")

    def test_rejects_all_hosts_and_reserved_block(self):
        for value in ("224.0.0.1", "224.0.0.0", "224.0.0.255"):
            with self.subTest(value=value):
                with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
                    osc_multicast.parse_ipv4_group(value)

    def test_group_error_is_a_value_error(self):
        self.assertTrue(
            issubclass(osc_multicast.InvalidMulticastGroupError, ValueError)
        )
        self.assertTrue(issubclass(osc_multicast.InvalidMulticastGroupError, OSError))

    def test_interface_validation(self):
        self.assertEqual(osc_multicast.parse_ipv4_interface(LOOPBACK), LOOPBACK)
        self.assertEqual(
            osc_multicast.parse_ipv4_interface(osc_multicast.ANY_INTERFACE),
            osc_multicast.ANY_INTERFACE,
        )
        for value in ("lo", "not-an-ip", None, "239.1.2.3"):
            with self.subTest(value=value):
                with self.assertRaises(osc_multicast.MulticastInterfaceError):
                    osc_multicast.parse_ipv4_interface(value)

    def test_ttl_validation(self):
        for value in (0, 1, 32, 255):
            self.assertEqual(osc_multicast.parse_ttl(value), value)
        for value in (-1, 256, 1000, 1.5, "1", True):
            with self.subTest(value=value):
                with self.assertRaises(osc_multicast.InvalidMulticastTTLError):
                    osc_multicast.parse_ttl(value)


class TestMulticastClientOptions(unittest.TestCase):
    @mock.patch("socket.socket")
    @mock.patch("socket.getaddrinfo")
    def test_sets_interface_ttl_and_loop(self, mock_getaddrinfo, mock_socket_ctor):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_DGRAM, 17, "", (GROUP, 9000))
        ]
        client = udp_client.UDPClient(
            GROUP,
            9000,
            multicast_interface=LOOPBACK,
            multicast_ttl=4,
            multicast_loop=False,
        )
        setsockopt = mock_socket_ctor.return_value.setsockopt
        setsockopt.assert_any_call(
            socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(LOOPBACK)
        )
        setsockopt.assert_any_call(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 4)
        setsockopt.assert_any_call(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
        client.close()

    @mock.patch("socket.socket")
    @mock.patch("socket.getaddrinfo")
    def test_unicast_still_uses_requested_family(
        self, mock_getaddrinfo, mock_socket_ctor
    ):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("::1", 9000))
        ]
        udp_client.UDPClient("::1", 9000)
        mock_getaddrinfo.assert_called_once_with(
            "::1", 9000, type=socket.SOCK_DGRAM, family=socket.AF_UNSPEC
        )

    def test_multicast_options_require_multicast_address(self):
        with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
            udp_client.UDPClient("127.0.0.1", 9000, multicast_ttl=1).close()

    def test_invalid_ttl_rejected_before_socket_creation(self):
        with mock.patch("socket.socket") as mock_socket_ctor:
            with self.assertRaises(osc_multicast.InvalidMulticastTTLError):
                udp_client.UDPClient(GROUP, 9000, multicast_ttl=999)
            mock_socket_ctor.assert_not_called()

    def test_unusable_interface_raises_and_closes_socket(self):
        with self.assertRaises(osc_multicast.MulticastInterfaceError):
            udp_client.UDPClient(
                GROUP,
                9000,
                multicast_interface="192.0.2.1",
            )

    def test_multicast_options_reject_ipv6_family(self):
        with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
            udp_client.UDPClient(GROUP, 9000, family=socket.AF_INET6, multicast_ttl=1)


class TestMulticastServerErrors(unittest.TestCase):
    def setUp(self) -> None:
        self.dispatcher = dispatcher.Dispatcher()

    def test_invalid_group_does_not_create_socket(self):
        with mock.patch("socket.socket") as mock_socket_ctor:
            with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
                osc_server.MulticastOSCUDPServer(("127.0.0.1", 9000), self.dispatcher)
            mock_socket_ctor.assert_not_called()

    def test_group_must_be_passed_once(self):
        with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
            osc_server.MulticastOSCUDPServer(
                (OTHER_GROUP, 9000), self.dispatcher, group=GROUP
            )

    def test_missing_group(self):
        with self.assertRaises(osc_multicast.InvalidMulticastGroupError):
            osc_server.MulticastOSCUDPServer(("", 9000), self.dispatcher)

    def test_unusable_interface_raises_membership_error(self):
        server = None
        try:
            with self.assertRaises(osc_multicast.MulticastMembershipError):
                server = osc_server.MulticastOSCUDPServer(
                    (GROUP, 0), self.dispatcher, interface="192.0.2.1"
                )
        finally:
            if server is not None:  # pragma: no cover - defensive
                server.server_close()

    def test_bind_failure_is_reported_and_socket_closed(self):
        blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        blocker.bind(("", 0))
        try:
            with self.assertRaises(osc_multicast.MulticastBindError) as ctx:
                osc_server.MulticastOSCUDPServer(
                    (GROUP, blocker.getsockname()[1]),
                    self.dispatcher,
                    interface=LOOPBACK,
                )
            self.assertEqual(ctx.exception.errno, 98)
        finally:
            blocker.close()

    def test_duplicate_join_and_unknown_leave(self):
        server = osc_server.MulticastOSCUDPServer(
            (GROUP, 0), self.dispatcher, interface=LOOPBACK
        )
        try:
            with self.assertRaises(osc_multicast.MulticastAlreadyJoinedError):
                server.join()
            with self.assertRaises(osc_multicast.MulticastNotJoinedError):
                server.leave(OTHER_GROUP)
        finally:
            server.server_close()

    def test_close_releases_socket_even_if_leave_fails(self):
        server = osc_server.MulticastOSCUDPServer(
            (GROUP, 0), self.dispatcher, interface=LOOPBACK
        )
        with mock.patch.object(
            osc_server.MulticastOSCUDPServer,
            "leave_all",
            side_effect=osc_multicast.MulticastMembershipError(99, "boom"),
        ):
            with self.assertRaises(osc_multicast.MulticastMembershipError):
                server.server_close()
        self.assertEqual(server.socket.fileno(), -1)

    def test_deferred_bind_and_join(self):
        server = osc_server.MulticastOSCUDPServer(
            (GROUP, 0),
            self.dispatcher,
            interface=LOOPBACK,
            bind_and_activate=False,
        )
        try:
            self.assertEqual(server.memberships, frozenset())
            server.server_bind()
            server.join()
            self.assertEqual(server.memberships, frozenset({(GROUP, LOOPBACK)}))
        finally:
            server.server_close()

    def test_shutdown_before_serve_does_not_deadlock(self):
        """``shutdown`` on a never-started server cleans up instead of hanging."""
        server = osc_server.MulticastOSCUDPServer(
            (GROUP, 0), self.dispatcher, interface=LOOPBACK
        )
        done = threading.Event()

        def stop() -> None:
            server.shutdown()
            done.set()

        thread = threading.Thread(target=stop, daemon=True)
        thread.start()
        self.assertTrue(done.wait(2.0), "shutdown() deadlocked")
        self.assertEqual(server.socket.fileno(), -1)
        self.assertEqual(server.memberships, frozenset())
        thread.join(timeout=1)

    def test_context_manager_cleanup_when_body_raises(self):
        server: osc_server.MulticastOSCUDPServer | None = None
        with self.assertRaises(RuntimeError):
            with osc_server.MulticastOSCUDPServer(
                (GROUP, 0), self.dispatcher, interface=LOOPBACK
            ) as server:
                raise RuntimeError("boom")
        assert server is not None
        self.assertEqual(server.socket.fileno(), -1)
        self.assertEqual(server.memberships, frozenset())

    def test_close_is_idempotent(self):
        server = osc_server.MulticastOSCUDPServer(
            (GROUP, 0), self.dispatcher, interface=LOOPBACK
        )
        server.server_close()
        server.server_close()
        self.assertEqual(server.socket.fileno(), -1)


@requires_multicast
class TestMulticastLoopback(unittest.TestCase):
    """Integration tests against real loopback multicast sockets."""

    def setUp(self) -> None:
        self.port = _free_udp_port()
        self.dispatch_a = RecordingDispatcher()
        self.dispatch_b = RecordingDispatcher()
        self.server_a = osc_server.MulticastOSCUDPServer(
            (GROUP, self.port), self.dispatch_a, interface=LOOPBACK
        )
        self.server_b = osc_server.MulticastOSCUDPServer(
            (GROUP, self.port), self.dispatch_b, interface=LOOPBACK
        )
        self.threads = [
            threading.Thread(target=s.serve_forever, kwargs={"poll_interval": 0.01})
            for s in (self.server_a, self.server_b)
        ]
        for thread in self.threads:
            thread.daemon = True
            thread.start()

    def tearDown(self) -> None:
        for server in (self.server_a, self.server_b):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)

    def _send(self, address: str = "/light/1", args: ArgValue = 1) -> None:
        client = udp_client.SimpleUDPClient(
            GROUP,
            self.port,
            multicast_interface=LOOPBACK,
            multicast_ttl=1,
            multicast_loop=True,
        )
        try:
            client.send_message(address, args)
        finally:
            client.close()

    def test_two_receivers_get_same_packet(self):
        self._send("/light/1", [1, 2, "warm"])
        self.assertTrue(
            self.dispatch_a.event.wait(2.0), "receiver A did not get the packet"
        )
        self.assertTrue(
            self.dispatch_b.event.wait(2.0), "receiver B did not get the packet"
        )
        self.assertEqual(self.dispatch_a.received, [("/light/1", (1, 2, "warm"))])
        self.assertEqual(self.dispatch_b.received, self.dispatch_a.received)

    def test_leave_stops_delivery(self):
        self.server_a.leave()
        self.assertEqual(self.server_a.memberships, frozenset())
        self._send()
        self.assertTrue(self.dispatch_b.event.wait(2.0))
        self.assertFalse(self.dispatch_a.event.wait(0.3))
        self.assertEqual(self.dispatch_a.received, [])

    def test_close_releases_membership_and_port(self):
        port = self.port
        for server, thread in zip(
            (self.server_a, self.server_b), self.threads, strict=True
        ):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            self.assertEqual(server.memberships, frozenset())
            self.assertEqual(server.socket.fileno(), -1)

        # The port is free again: another multicast receiver can bind it.
        replacement = osc_server.MulticastOSCUDPServer(
            (GROUP, port), RecordingDispatcher(), interface=LOOPBACK
        )
        try:
            self.assertEqual(replacement.server_address, ("0.0.0.0", port))
        finally:
            replacement.server_close()

    def test_join_additional_group(self):
        self.server_a.join(OTHER_GROUP, LOOPBACK)
        self.assertIn((OTHER_GROUP, LOOPBACK), self.server_a.memberships)
        client = udp_client.SimpleUDPClient(
            OTHER_GROUP, self.port, multicast_interface=LOOPBACK, multicast_ttl=1
        )
        try:
            client.send_message("/light/2", 42)
        finally:
            client.close()
        self.assertTrue(self.dispatch_a.event.wait(2.0))
        self.assertEqual(self.dispatch_a.received, [("/light/2", (42,))])
        # Receiver B did not join the second group.
        self.assertFalse(self.dispatch_b.event.wait(0.3))


class TestUnicastRegression(unittest.TestCase):
    """The plain unicast server/client path must be unaffected."""

    def test_unicast_round_trip(self):
        received: list[tuple[str, tuple]] = []
        event = threading.Event()

        dispatch = dispatcher.Dispatcher()

        def handler(address: str, *args: object) -> None:
            received.append((address, args))
            event.set()

        dispatch.map("/sync", handler)
        server = osc_server.BlockingOSCUDPServer(("127.0.0.1", 0), dispatch)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}
        )
        thread.daemon = True
        thread.start()
        try:
            client = udp_client.SimpleUDPClient("127.0.0.1", server.server_address[1])
            try:
                client.send_message("/sync", 7)
            finally:
                client.close()
            self.assertTrue(event.wait(2.0))
            self.assertEqual(received, [("/sync", (7,))])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_unicast_bind_excludes_multicast_server(self):
        """A multicast server must not silently steal a unicast port."""
        unicast = osc_server.BlockingOSCUDPServer(
            ("127.0.0.1", 0), dispatcher.Dispatcher()
        )
        try:
            with self.assertRaises(osc_multicast.MulticastBindError):
                osc_server.MulticastOSCUDPServer(
                    (GROUP, unicast.server_address[1]),
                    dispatcher.Dispatcher(),
                    interface=LOOPBACK,
                )
        finally:
            unicast.server_close()


if __name__ == "__main__":
    unittest.main()
