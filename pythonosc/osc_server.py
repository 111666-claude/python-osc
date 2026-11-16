"""OSC Servers that receive UDP packets and invoke handlers accordingly."""

import asyncio
import errno
import os
import socket
import socketserver
from socket import socket as _socket
from typing import Any, Coroutine, Tuple, Union, cast

from pythonosc import osc_bundle, osc_message, osc_multicast
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_message_builder import build_msg

_RequestType = Union[_socket, Tuple[bytes, _socket]]
_AddressType = Union[Tuple[str, int], str]


class _UDPHandler(socketserver.BaseRequestHandler):
    """Handles correct UDP messages for all types of server."""

    def __init__(self, request, client_address, server):
        self.socket = request[1]
        super().__init__(request, client_address, server)

    def handle(self) -> None:
        """Calls the handlers via dispatcher

        This method is called after a basic sanity check was done on the datagram,
        whether this datagram looks like an osc message or bundle.
        If not the server won't call it and so no new
        threads/processes will be spawned.
        """
        server = cast(OSCUDPServer, self.server)
        resp = server.dispatcher.call_handlers_for_packet(
            self.request[0], self.client_address
        )
        for r in resp:
            if not isinstance(r, tuple):
                r = [r]
            msg = build_msg(r[0], r[1:])
            self.socket.sendto(msg.dgram, self.client_address)


def _is_valid_request(request: _RequestType) -> bool:
    """Returns true if the request's data looks like an osc bundle or message.

    Returns:
        True if request is OSC bundle or OSC message
    """
    assert isinstance(
        request, tuple
    )  # TODO: handle requests which are passed just as a socket?
    data = request[0]
    return osc_bundle.OscBundle.dgram_is_bundle(
        data
    ) or osc_message.OscMessage.dgram_is_message(data)


class OSCUDPServer(socketserver.UDPServer):
    """Superclass for different flavors of OSC UDP servers"""

    # Receive complete UDP datagrams instead of socketserver's 8192-byte default.
    max_packet_size = 65535

    def __init__(
        self,
        server_address: Tuple[str, int],
        dispatcher: Dispatcher,
        bind_and_activate: bool = True,
        timeout: float | None = None,
        family: socket.AddressFamily | None = None,
    ) -> None:
        """Initialize

        Args:
            server_address: IP and port of server
            dispatcher: Dispatcher this server will use
            (optional) bind_and_activate: default=True defines if the server has to start on call of constructor
            (optional) timeout: Default timeout in seconds for socket operations
            (optional) family: socket.AF_INET or socket.AF_INET6. If None, it will be inferred from server_address.
        """
        if family is not None:
            self.address_family = family
        else:
            # Try to infer address family from server_address
            try:
                infos = socket.getaddrinfo(
                    server_address[0],
                    server_address[1],
                    type=socket.SOCK_DGRAM,
                    family=socket.AF_UNSPEC,
                )
                if infos:
                    self.address_family = infos[0][0]
            except (socket.gaierror, IndexError):
                # Fallback to default if resolution fails
                pass

        super().__init__(server_address, _UDPHandler, bind_and_activate)
        self._dispatcher = dispatcher
        self.timeout = timeout

    def verify_request(
        self, request: _RequestType, client_address: _AddressType
    ) -> bool:
        """Returns true if the data looks like a valid OSC UDP datagram

        Args:
            request: Incoming data
            client_address: IP and port of client this message came from

        Returns:
            True if request is OSC bundle or OSC message
        """
        return _is_valid_request(request)

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher


class BlockingOSCUDPServer(OSCUDPServer):
    """Blocking version of the UDP server.

    Each message will be handled sequentially on the same thread.
    Use this is you don't care about latency in your message handling or don't
    have a multiprocess/multithread environment.
    """


class ThreadingOSCUDPServer(socketserver.ThreadingMixIn, OSCUDPServer):
    """Threading version of the OSC UDP server.

    Each message will be handled in its own new thread.
    Use this when lightweight operations are done by each message handlers.
    """


class MulticastOSCUDPServer(OSCUDPServer):
    """OSC UDP server that receives from an IPv4 multicast group.

    The server binds its socket to ``("", port)`` so that it can receive
    datagrams addressed to the multicast group, then joins the group on the
    requested local interface.  Several servers -- in one process or across
    several processes -- may join the same group and port and each of them
    receives a copy of every datagram.

    The socket is opened with ``SO_REUSEADDR``.  This is required for more than
    one receive-only socket to bind the same multicast port on the same host,
    and matches the behavior of other multicast receivers.  It does *not* allow
    this server to coexist with an ordinary ``OSCUDPServer`` already bound to
    the same port: that combination still fails loudly with
    :class:`MulticastBindError`.

    Memberships can be added after construction with :meth:`join` and are
    removed by :meth:`leave` or, for memberships added through the constructor,
    by :meth:`shutdown`/:meth:`server_close`.  Closing the server always
    closes the underlying socket, even when leaving a group fails.

    Example::

        dispatcher = Dispatcher()
        dispatcher.map("/light/*", print)
        with MulticastOSCUDPServer(("239.255.0.1", 9000), dispatcher) as server:
            server.serve_forever()
    """

    allow_reuse_address = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        dispatcher: Dispatcher,
        group: str | None = None,
        interface: str = osc_multicast.ANY_INTERFACE,
        bind_and_activate: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Initialize the server and join the multicast group.

        Args:
            server_address: ``(group, port)`` for the common case, or
                ``("", port)`` when ``group`` is given separately.  The group
                is never bound as a local address.
            dispatcher: Dispatcher this server will use.
            group: Multicast group address.  May be omitted only when
                ``server_address[0]`` is a multicast address.
            interface: Local IPv4 address of the interface that should join the
                group.  ``"0.0.0.0"`` (the default) lets the OS choose.
            bind_and_activate: default=True defines if the server has to start
                on call of constructor.  When False, the socket is created but
                neither bound nor joined; call :meth:`server_bind` and
                :meth:`join` explicitly.
            timeout: Default timeout in seconds for socket operations.

        Raises:
            InvalidMulticastGroupError: if the group is not a usable multicast
                address.
            MulticastInterfaceError: if ``interface`` is not a local IPv4
                address.
            MulticastBindError: if the local port cannot be bound.
            MulticastMembershipError: if the group cannot be joined -- for
                example because the interface does not exist.
        """
        # Only IPv4 multicast is supported; make sure OSCUDPServer's address
        # family inference cannot override this.
        self.address_family = socket.AF_INET
        self._multicast_group: str | None = None
        self._multicast_interface: str = osc_multicast.ANY_INTERFACE
        self._memberships: set[Tuple[str, str]] = set()
        self._serving = False

        address_group = server_address[0]
        if group is None:
            if not address_group:
                raise osc_multicast.InvalidMulticastGroupError(
                    "a multicast group address is required"
                )
            group = address_group
            bind_address = ""
        else:
            if address_group not in ("", "0.0.0.0"):
                raise osc_multicast.InvalidMulticastGroupError(
                    "server_address must be ('', port) when group is passed "
                    f"separately, got {address_group!r}"
                )
            bind_address = ""

        self._multicast_group = osc_multicast.parse_ipv4_group(group)
        self._multicast_interface = osc_multicast.parse_ipv4_interface(interface)

        super().__init__(
            (bind_address, server_address[1]),
            dispatcher,
            bind_and_activate=bind_and_activate,
            timeout=timeout,
            family=socket.AF_INET,
        )

        if bind_and_activate:
            try:
                self.join(self._multicast_group, self._multicast_interface)
            except BaseException:
                # Never leak the bound socket if the group cannot be joined.
                self.server_close()
                raise

    def server_bind(self) -> None:
        """Bind the receiving port, translating failures to a typed error."""
        try:
            super().server_bind()
        except OSError as exc:
            raise osc_multicast.MulticastBindError(
                exc.errno,
                f"cannot bind multicast receiver to port "
                f"{self.server_address[1]}: {exc.strerror}",
            ) from exc
        # Must happen before the first join.  Otherwise, on Linux, the socket
        # also receives groups joined elsewhere on the host, so per-server
        # membership and leave() would have no effect.
        osc_multicast.restrict_to_joined_groups(self.socket)

    @property
    def multicast_group(self) -> str:
        """The group this server was configured with."""
        assert self._multicast_group is not None
        return self._multicast_group

    @property
    def multicast_interface(self) -> str:
        """The interface used for the initial group membership."""
        return self._multicast_interface

    @property
    def memberships(self) -> frozenset[Tuple[str, str]]:
        """Currently active ``(group, interface)`` memberships."""
        return frozenset(self._memberships)

    def join(
        self,
        group: str | None = None,
        interface: str | None = None,
    ) -> None:
        """Join an additional multicast group on this server's socket.

        Args:
            group: Group to join.  Defaults to the group passed to the
                constructor.
            interface: Local IPv4 interface address, or ``"0.0.0.0"`` to let
                the OS choose.  Defaults to the interface passed to the
                constructor.

        Raises:
            InvalidMulticastGroupError: if ``group`` is not a usable group.
            MulticastInterfaceError: if ``interface`` is invalid.
            MulticastAlreadyJoinedError: if the exact group/interface pair is
                already joined on this socket.
            MulticastMembershipError: if the OS rejects the membership.
        """
        if group is None:
            group = self.multicast_group
        else:
            group = osc_multicast.parse_ipv4_group(group)
        if interface is None:
            interface = self._multicast_interface
        else:
            interface = osc_multicast.parse_ipv4_interface(interface)

        key = (group, interface)
        if key in self._memberships:
            raise osc_multicast.MulticastAlreadyJoinedError(
                errno.EADDRINUSE,
                f"already joined group {group} on interface {interface}",
            )

        try:
            self.socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                b"".join(osc_multicast.membership_request(group, interface)),
            )
        except OSError as exc:
            raise osc_multicast.MulticastMembershipError(
                exc.errno,
                f"cannot join multicast group {group} on interface "
                f"{interface}: {exc.strerror}",
            ) from exc
        self._memberships.add(key)

    def leave(
        self,
        group: str | None = None,
        interface: str | None = None,
    ) -> None:
        """Leave a multicast group joined through :meth:`join` or the constructor.

        Args:
            group: Group to leave.  Defaults to the group passed to the
                constructor.
            interface: Interface the group was joined on.  Defaults to the
                interface passed to the constructor.

        Raises:
            InvalidMulticastGroupError: if ``group`` is not a usable group.
            MulticastInterfaceError: if ``interface`` is invalid.
            MulticastNotJoinedError: if the group/interface pair is not joined
                on this socket.
            MulticastMembershipError: if the OS rejects the drop request.
        """
        if group is None:
            group = self.multicast_group
        else:
            group = osc_multicast.parse_ipv4_group(group)
        if interface is None:
            interface = self._multicast_interface
        else:
            interface = osc_multicast.parse_ipv4_interface(interface)

        key = (group, interface)
        if key not in self._memberships:
            raise osc_multicast.MulticastNotJoinedError(
                errno.EADDRNOTAVAIL,
                f"group {group} is not joined on interface {interface}",
            )

        try:
            self.socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_DROP_MEMBERSHIP,
                b"".join(osc_multicast.membership_request(group, interface)),
            )
        except OSError as exc:
            raise osc_multicast.MulticastMembershipError(
                exc.errno,
                f"cannot leave multicast group {group} on interface "
                f"{interface}: {exc.strerror}",
            ) from exc
        self._memberships.discard(key)

    def leave_all(self) -> None:
        """Leave every group this server joined.

        Every membership is attempted even if one of them fails; the first
        failure is re-raised afterwards so cleanup of the remaining groups
        still happens.  Bookkeeping is cleared regardless, because the kernel
        drops all memberships when the socket is closed and a later
        :meth:`server_close` must not retry against a dead socket.
        """
        first_error: osc_multicast.MulticastError | None = None
        for group, interface in list(self._memberships):
            try:
                self.leave(group, interface)
            except osc_multicast.MulticastError as exc:
                self._memberships.discard((group, interface))
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def shutdown(self) -> None:
        """Stop serving (if running), leave every group and close the socket.

        ``socketserver.BaseServer.shutdown`` deadlocks when it is called
        before :meth:`serve_forever` -- exactly what happens when a ``with
        MulticastOSCUDPServer(...)`` body raises.  Instead of inheriting that
        trap, a server that was never started is cleaned up immediately and a
        running server is stopped normally; in both cases the groups are left
        and the socket is closed.  The method is safe to call more than once.
        """
        if self._serving:
            super().shutdown()
        else:
            # Unblock a serve_forever() that may be starting concurrently.
            self._BaseServer__shutdown_request = True
        self.leave_all()
        self.server_close()

    def serve_forever(self, poll_interval: float = 0.5) -> None:
        """Serve until :meth:`shutdown` is called.

        In addition to the base implementation this tracks whether the loop is
        actually running, so :meth:`shutdown` never waits forever on a server
        that was never started.
        """
        self._serving = True
        try:
            super().serve_forever(poll_interval)
        finally:
            self._serving = False

    def server_close(self) -> None:
        """Leave all groups and close the socket.

        Safe to call more than once.  An exception raised while leaving a
        group is re-raised *after* the socket is closed, so resources are never
        leaked.
        """
        try:
            self.leave_all()
        finally:
            super().server_close()


if hasattr(os, "fork"):

    class ForkingOSCUDPServer(socketserver.ForkingMixIn, OSCUDPServer):
        """Forking version of the OSC UDP server.

        Each message will be handled in its own new process.
        Use this when heavyweight operations are done by each message handlers
        and forking a whole new process for each of them is worth it.
        """


class AsyncIOOSCUDPServer:
    """Asynchronous OSC Server

    An asynchronous OSC Server using UDP. It creates a datagram endpoint that runs in an event loop.
    """

    def __init__(
        self,
        server_address: Tuple[str, int],
        dispatcher: Dispatcher,
        loop: asyncio.BaseEventLoop,
    ) -> None:
        """Initialize

        Args:
            server_address: IP and port of server
            dispatcher: Dispatcher this server shall use
            loop: Event loop to add the server task to. Use ``asyncio.get_event_loop()`` unless you know what you're
                doing.
        """

        self._server_address = server_address
        self._dispatcher = dispatcher
        self._loop = loop

    class _OSCProtocolFactory(asyncio.DatagramProtocol):
        """OSC protocol factory which passes datagrams to dispatcher"""

        def __init__(self, dispatcher: Dispatcher) -> None:
            self.dispatcher = dispatcher

        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(
            self, data: bytes, client_address: Tuple[str, int]
        ) -> None:
            resp = self.dispatcher.call_handlers_for_packet(data, client_address)
            for r in resp:
                if not isinstance(r, tuple):
                    r = [r]
                msg = build_msg(r[0], r[1:])
                self.transport.sendto(msg.dgram, client_address)

    def serve(self) -> None:
        """Creates a datagram endpoint and registers it with event loop.

        Use this only in synchronous code (i.e. not from within a coroutine). This will start the server and run it
        forever or until a ``stop()`` is called on the event loop.
        """
        self._loop.run_until_complete(self.create_serve_endpoint())

    def create_serve_endpoint(
        self,
    ) -> Coroutine[
        Any, Any, Tuple[asyncio.transports.BaseTransport, asyncio.DatagramProtocol]
    ]:
        """Creates a datagram endpoint and registers it with event loop as coroutine.

        Returns:
            Awaitable coroutine that returns transport and protocol objects
        """
        return self._loop.create_datagram_endpoint(
            lambda: self._OSCProtocolFactory(self.dispatcher),
            local_addr=self._server_address,
        )

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher
