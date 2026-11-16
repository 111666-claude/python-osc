"""UDP Clients for sending OSC messages to an OSC server"""

import sys

if sys.version_info > (3, 5):
    from collections.abc import Iterable
else:
    from collections import Iterable

import socket
from typing import Generator, Union

from pythonosc import osc_multicast
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_bundle import OscBundle
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue, OscMessageBuilder


class UDPClient(object):
    """OSC client to send :class:`OscMessage` or :class:`OscBundle` via UDP

    Sending to an IPv4 multicast group works with the same API.  Pass
    ``multicast_interface`` to select the outgoing network interface and
    ``multicast_ttl`` to control how far the datagrams may travel.  With plain
    unicast addresses those options are rejected rather than silently ignored.
    """

    def __init__(
        self,
        address: str,
        port: int,
        allow_broadcast: bool = False,
        family: socket.AddressFamily = socket.AF_UNSPEC,
        timeout: float | None = None,
        multicast_interface: str | None = None,
        multicast_ttl: int | None = None,
        multicast_loop: bool | None = None,
    ) -> None:
        """Initialize client

        As this is UDP it will not actually make any attempt to connect to the
        given server at ip:port until the send() method is called.

        Args:
            address: IP address of server, or IPv4 multicast group
            port: Port of server
            allow_broadcast: Allow for broadcast transmissions
            family: address family parameter (passed to socket.getaddrinfo)
            timeout: Default timeout in seconds for socket operations
            multicast_interface: Local IPv4 interface address used to send to a
                multicast group.  ``"0.0.0.0"`` lets the OS choose.  Only valid
                with an IPv4 multicast ``address``.
            multicast_ttl: IPv4 multicast TTL (0-255).  Only valid with an
                IPv4 multicast ``address``.
            multicast_loop: Whether multicast datagrams are looped back to this
                host's receivers.  Defaults to the OS setting (usually on).
                Only valid with an IPv4 multicast ``address``.

        Raises:
            InvalidMulticastGroupError: if multicast options are combined with
                a non-multicast address.
            MulticastInterfaceError: if ``multicast_interface`` is invalid.
            InvalidMulticastTTLError: if ``multicast_ttl`` is not in 0-255.
            MulticastInterfaceError: if the OS cannot use the interface.
        """
        multicast_options_given = (
            multicast_interface is not None
            or multicast_ttl is not None
            or multicast_loop is not None
        )
        self._multicast_group: str | None = None
        if multicast_options_given:
            group = osc_multicast.parse_ipv4_group(address)
            if family not in (socket.AF_UNSPEC, socket.AF_INET):
                raise osc_multicast.InvalidMulticastGroupError(
                    "IPv4 multicast requires family socket.AF_INET"
                )
            family = socket.AF_INET
            self._multicast_group = group
            if multicast_interface is not None:
                multicast_interface = osc_multicast.parse_ipv4_interface(
                    multicast_interface
                )
            if multicast_ttl is not None:
                multicast_ttl = osc_multicast.parse_ttl(multicast_ttl)

        for addr in socket.getaddrinfo(
            address, port, type=socket.SOCK_DGRAM, family=family
        ):
            af, socktype, protocol, canonname, sa = addr

            try:
                self._sock = socket.socket(af, socktype)
            except OSError:
                continue
            break

        self._sock.setblocking(False)
        if timeout is not None:
            self._sock.settimeout(timeout)
        self._timeout = timeout

        if allow_broadcast:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self._multicast_group is not None:
            try:
                if multicast_interface is not None:
                    self._sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(multicast_interface),
                    )
            except OSError as exc:
                self._sock.close()
                raise osc_multicast.MulticastInterfaceError(
                    exc.errno,
                    f"cannot use interface {multicast_interface} for multicast: "
                    f"{exc.strerror}",
                ) from exc
            try:
                if multicast_ttl is not None:
                    self._sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_TTL,
                        multicast_ttl,
                    )
                if multicast_loop is not None:
                    self._sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_LOOP,
                        1 if multicast_loop else 0,
                    )
            except OSError as exc:
                self._sock.close()
                raise osc_multicast.MulticastError(
                    exc.errno,
                    f"cannot configure multicast options: {exc.strerror}",
                ) from exc

        self._address = address
        self._port = port

    def __enter__(self) -> "UDPClient":
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the socket"""
        self._sock.close()

    def send(self, content: Union[OscMessage, OscBundle]) -> None:
        """Sends an :class:`OscMessage` or :class:`OscBundle` via UDP

        Args:
            content: Message or bundle to be sent
        """
        self._sock.sendto(content.dgram, (self._address, self._port))

    def receive(self, timeout: float | None = None) -> bytes:
        """Wait :int:`timeout` seconds for a message an return the raw bytes

        Args:
            timeout: Number of seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        if timeout is not None:
            self._sock.settimeout(timeout)
        elif self._timeout is not None:
            self._sock.settimeout(self._timeout)

        try:
            return self._sock.recv(4096)
        except (TimeoutError, socket.timeout, BlockingIOError):
            return b""


class SimpleUDPClient(UDPClient):
    """Simple OSC client that automatically builds :class:`OscMessage` from arguments"""

    def send_message(
        self, address: str, value: Union[ArgValue, Iterable[ArgValue]]
    ) -> None:
        """Build :class:`OscMessage` from arguments and send to server

        Args:
            address: OSC address the message shall go to
            value: One or more arguments to be added to the message
        """
        builder = OscMessageBuilder(address=address)
        if value is None:
            pass
        elif not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
            builder.add_arg(value)
        else:
            for val in value:
                builder.add_arg(val)
        msg = builder.build()
        self.send(msg)

    def get_messages(self, timeout: float | None = None) -> Generator:
        """Wait :int:`timeout` seconds for a message from the server and convert it to a :class:`OscMessage`

        Args:
            timeout: Time in seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        msg = self.receive(timeout)
        while msg:
            yield OscMessage(msg)
            msg = self.receive(timeout)


class DispatchClient(SimpleUDPClient):
    """OSC Client that includes a :class:`Dispatcher` for handling responses and other messages from the server"""

    dispatcher = Dispatcher()

    def handle_messages(self, timeout: float | None = None) -> None:
        """Wait :int:`timeout` seconds for a message from the server and process each message with the registered
        handlers.  Continue until a timeout occurs.

        Args:
            timeout: Time in seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        msg = self.receive(timeout)
        while msg:
            self.dispatcher.call_handlers_for_packet(msg, (self._address, self._port))
            msg = self.receive(timeout)
