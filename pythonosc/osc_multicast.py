"""Shared IPv4 multicast helpers for OSC clients and servers.

The helpers in this module validate user input before it ever reaches
``setsockopt`` so that configuration mistakes produce actionable exceptions
instead of raw ``OSError`` values.
"""

import ipaddress
import socket
import sys
from typing import Tuple

#: Linux ``IP_MULTICAST_ALL``.  CPython does not expose this constant, but the
#: option is stable at 49 for the Linux ABI.
_LINUX_IP_MULTICAST_ALL = 49

# 224.0.0.0/24 is the local network control block and must not be used by
# ordinary applications (RFC 5771).  It is also the block operating systems
# treat specially, so a group in it is almost certainly a configuration error.
_RESERVED_LINK_LOCAL = ipaddress.IPv4Network("224.0.0.0/24")

#: Value accepted by the interface argument to mean "let the OS choose".
ANY_INTERFACE = "0.0.0.0"


class MulticastError(OSError):
    """Base class for errors raised by the IPv4 multicast helpers."""


class InvalidMulticastGroupError(MulticastError, ValueError):
    """Raised when an address is not a usable IPv4 multicast group."""


class InvalidMulticastTTLError(MulticastError, ValueError):
    """Raised when a TTL is outside the IPv4 multicast range 0-255."""


class MulticastInterfaceError(MulticastError):
    """Raised when a local interface cannot be used for multicast."""


class MulticastMembershipError(MulticastError):
    """Raised when a group membership cannot be added or removed."""


class MulticastAlreadyJoinedError(MulticastMembershipError):
    """Raised when joining a group/interface pair that is already joined."""


class MulticastNotJoinedError(MulticastMembershipError):
    """Raised when leaving a group/interface pair that is not joined."""


class MulticastBindError(MulticastError):
    """Raised when the receiving socket cannot bind its local port."""


def parse_ipv4_group(group: str) -> str:
    """Validate and normalize an IPv4 multicast group address.

    Args:
        group: Dotted-quad IPv4 address.

    Returns:
        The address in canonical dotted-quad form.

    Raises:
        InvalidMulticastGroupError: if ``group`` is not resolvable to a unicast
            or multicast IPv4 address, is not a multicast address, is the
            all-hosts group, or is in the reserved 224.0.0.0/24 block.
    """
    try:
        address = ipaddress.IPv4Address(group)
    except (ipaddress.AddressValueError, ValueError, TypeError) as exc:
        raise InvalidMulticastGroupError(
            f"{group!r} is not a valid IPv4 address"
        ) from exc

    if not address.is_multicast:
        raise InvalidMulticastGroupError(f"{address} is not a multicast group")

    # 224.0.0.1 (all hosts on the local subnet) is permanently joined on many
    # platforms and joining it explicitly either fails or changes host-wide
    # behavior, so it is never a valid argument here.
    if address == ipaddress.IPv4Address("224.0.0.1"):
        raise InvalidMulticastGroupError(
            f"{address} is the all-hosts group and cannot be used"
        )
    if address in _RESERVED_LINK_LOCAL:
        raise InvalidMulticastGroupError(
            f"{address} is in the reserved local network control block 224.0.0.0/24"
        )
    return str(address)


def parse_ipv4_interface(interface: str) -> str:
    """Validate and normalize a local IPv4 interface address.

    Only addresses are accepted; interface names are intentionally not
    resolved because doing so portably requires platform-specific APIs.
    The unspecified address ``0.0.0.0`` is accepted and means "let the
    operating system select the interface".

    Raises:
        MulticastInterfaceError: if ``interface`` is not an IPv4 address or is
            itself a multicast address.
    """
    try:
        address = ipaddress.IPv4Address(interface)
    except (ipaddress.AddressValueError, ValueError, TypeError) as exc:
        raise MulticastInterfaceError(
            f"{interface!r} is not a valid IPv4 interface address"
        ) from exc
    if address.is_multicast:
        raise MulticastInterfaceError(
            f"{address} is a multicast group, not a local interface"
        )
    return str(address)


def parse_ttl(ttl: int) -> int:
    """Validate an IPv4 multicast TTL.

    Raises:
        InvalidMulticastTTLError: if ``ttl`` is not an integer in 0..255.
    """
    if isinstance(ttl, bool) or not isinstance(ttl, int):
        raise InvalidMulticastTTLError(f"TTL must be an integer, got {ttl!r}")
    if not 0 <= ttl <= 255:
        raise InvalidMulticastTTLError(f"TTL must be between 0 and 255, got {ttl}")
    return ttl


def membership_request(group: str, interface: str) -> Tuple[bytes, bytes]:
    """Build the ``ip_mreq`` payload used to join or leave ``group``.

    Args:
        group: Validated multicast group address.
        interface: Validated local interface address, or :data:`ANY_INTERFACE`.
    """
    return socket.inet_aton(group), socket.inet_aton(interface)


def restrict_to_joined_groups(sock: socket.socket) -> None:
    """Deliver only groups this socket joined, not every group of the host.

    On Linux every ``IP_ADD_MEMBERSHIP`` is recorded host-wide unless
    ``IP_MULTICAST_ALL`` is disabled.  The default (1) means a socket bound to
    the multicast port receives datagrams for *any* group that some other
    socket or process on the host joined, which defeats per-server membership
    and makes :meth:`MulticastOSCUDPServer.leave` ineffective.  Platforms
    without the option already behave this way, so failure to set it is not
    treated as fatal.
    """
    if sys.platform.startswith("linux"):
        option = _LINUX_IP_MULTICAST_ALL
    else:
        option = getattr(socket, "IP_MULTICAST_ALL", None)
    if option is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_IP, option, 0)
    except OSError:
        pass
