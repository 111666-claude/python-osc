"""Read-only OSCQuery-style address discovery service.

Builds a JSON address tree from a :class:`~pythonosc.dispatcher.Dispatcher`'s
explicit address registrations and serves it over local HTTP, so control
software can list the OSC addresses a device accepts before generating a
control panel.

The service is strictly read-only: it never invokes control callbacks and
never mutates the dispatcher. Registration and unregistration on the
dispatcher are reflected in subsequent queries.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from pythonosc.dispatcher import Dispatcher

#: Values accepted for the OSCQuery ACCESS metadata field.
ACCESS_VALUES: Tuple[str, ...] = ("read", "write", "readwrite")

#: Default upper bound on the number of nodes (root + containers + leaves)
#: a served address tree may contain.
DEFAULT_MAX_NODES = 10_000

# Characters that may not appear in a registered OSC address. Wildcards
# (* ? [ ] { }) stay valid because the dispatcher supports matching them;
# they are represented literally in the discovery tree.
_INVALID_ADDRESS_CHARS = frozenset(" #")

# Characters that may not appear in an HTTP query path. Queries are literal
# paths, so OSC wildcards are rejected there.
_INVALID_QUERY_CHARS = frozenset(" #*?[]{}")


class OscQueryError(Exception):
    """Base class for OSCQuery discovery errors."""


class OscQueryTreeTooLargeError(OscQueryError):
    """The registered address tree exceeds the configured node limit."""


@dataclass(frozen=True)
class OscQueryMetadata:
    """Optional OSCQuery metadata attached to an explicitly mapped address.

    Attributes:
        type: OSC type tag string of the accepted arguments (e.g. "f", "ifs").
        description: Human-readable description of the address.
        access: One of "read", "write", "readwrite" (see ACCESS_VALUES).
    """

    type: Optional[str] = None
    description: Optional[str] = None
    access: Optional[str] = None


def validate_osc_address(address: str) -> None:
    """Validate an OSC address for inclusion in the discovery tree.

    Raises:
        ValueError: With a specific message for each rejection reason.
    """
    if not isinstance(address, str) or not address:
        raise ValueError(f"OSC address must be a non-empty string, got {address!r}")
    if not address.startswith("/"):
        raise ValueError(f"OSC address '{address}' must start with '/'")
    if address == "/":
        raise ValueError("OSC address '/' must name a node below the root")
    if address.endswith("/") or "//" in address:
        raise ValueError(f"OSC address '{address}' contains an empty path segment")
    for ch in address:
        if ch in _INVALID_ADDRESS_CHARS or not "!" <= ch <= "~":
            raise ValueError(
                f"OSC address '{address}' contains invalid character {ch!r}"
            )


def _validate_query_path(path: str) -> Optional[str]:
    """Return an error description if a query path is invalid, else None."""
    if not path.startswith("/"):
        return f"query path '{path}' must start with '/'"
    if path != "/" and (path.endswith("/") or "//" in path):
        return f"query path '{path}' contains an empty path segment"
    for ch in path:
        if ch in _INVALID_QUERY_CHARS or not "!" <= ch <= "~":
            return f"query path '{path}' contains invalid character {ch!r}"
    return None


def _prune_empty_contents(node: Dict[str, Any]) -> None:
    contents = node["CONTENTS"]
    for child in contents.values():
        _prune_empty_contents(child)
    if not contents:
        del node["CONTENTS"]


def build_address_tree(
    addresses: List[str],
    metadata: Dict[str, OscQueryMetadata],
    max_nodes: int = DEFAULT_MAX_NODES,
) -> Dict[str, Any]:
    """Build a JSON-serializable OSCQuery-style tree from registered addresses.

    Args:
        addresses: Sorted list of explicitly registered OSC addresses.
        metadata: Mapping of address to its optional OscQueryMetadata.
        max_nodes: Maximum number of tree nodes (root included).

    Returns:
        Nested dict with FULL_PATH / CONTENTS keys, plus TYPE / DESCRIPTION /
        ACCESS on nodes whose address carries metadata.

    Raises:
        OscQueryTreeTooLargeError: If the tree would exceed max_nodes nodes.
    """
    root: Dict[str, Any] = {"FULL_PATH": "/", "CONTENTS": {}}
    nodes = 1
    for address in addresses:
        node = root
        full_path = ""
        for segment in address[1:].split("/"):
            full_path += "/" + segment
            contents = node["CONTENTS"]
            if segment not in contents:
                nodes += 1
                if nodes > max_nodes:
                    raise OscQueryTreeTooLargeError(
                        f"address tree exceeds the limit of {max_nodes} nodes"
                    )
                contents[segment] = {"FULL_PATH": full_path, "CONTENTS": {}}
            node = contents[segment]
        meta = metadata.get(address)
        if meta is not None:
            if meta.type is not None:
                node["TYPE"] = meta.type
            if meta.description is not None:
                node["DESCRIPTION"] = meta.description
            if meta.access is not None:
                node["ACCESS"] = meta.access
    _prune_empty_contents(root)
    return root


def _lookup(tree: Dict[str, Any], path: str) -> Optional[Dict[str, Any]]:
    """Return the subtree rooted at path, or None if path is not registered."""
    node = tree
    if path != "/":
        for segment in path[1:].split("/"):
            contents = node.get("CONTENTS")
            if not contents or segment not in contents:
                return None
            node = contents[segment]
    return node


class _OscQueryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        service: "OscQueryService",
    ) -> None:
        super().__init__(server_address, _OscQueryRequestHandler)
        self.oscquery_service = service


class _OscQueryRequestHandler(BaseHTTPRequestHandler):
    server_version = "python-osc-oscquery"

    @property
    def _service(self) -> "OscQueryService":
        return self.server.oscquery_service  # type: ignore[attr-defined]

    def do_GET(self) -> None:
        self._serve(include_body=True)

    def do_HEAD(self) -> None:
        self._serve(include_body=False)

    def do_POST(self) -> None:
        self._method_not_allowed()

    def do_PUT(self) -> None:
        self._method_not_allowed()

    def do_DELETE(self) -> None:
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        self._respond(405, {"error": "discovery service is read-only, use GET"})

    def _serve(self, include_body: bool) -> None:
        try:
            path = urllib.parse.unquote(
                urllib.parse.urlsplit(self.path).path, errors="strict"
            )
        except (ValueError, UnicodeDecodeError):
            self._respond(400, {"error": "malformed request path"}, include_body)
            return
        problem = _validate_query_path(path)
        if problem is not None:
            self._respond(400, {"error": problem}, include_body)
            return
        try:
            tree = self._service.build_tree()
        except OscQueryTreeTooLargeError as exc:
            self._respond(413, {"error": str(exc)}, include_body)
            return
        node = _lookup(tree, path)
        if node is None:
            self._respond(
                404, {"error": f"no OSC address registered at '{path}'"}, include_body
            )
            return
        self._respond(200, node, include_body)

    def _respond(
        self, status: int, payload: Dict[str, Any], include_body: bool = True
    ) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Keep discovery traffic out of the application logs.


class OscQueryService:
    """Serves a read-only JSON view of a Dispatcher's registered addresses.

    The tree is rebuilt from the dispatcher's current registrations on every
    query, so map()/unmap() calls are reflected immediately. Each request
    operates on a consistent snapshot, making concurrent registration and
    querying safe.

    Args:
        dispatcher: The dispatcher whose explicit registrations are exposed.
        host: Interface to bind; defaults to loopback only.
        port: TCP port to bind; 0 (default) picks a free ephemeral port.
        max_nodes: Maximum address-tree size; larger trees are rejected with
            a defined error (HTTP 413) instead of being served.
    """

    def __init__(
        self,
        dispatcher: "Dispatcher",
        host: str = "127.0.0.1",
        port: int = 0,
        max_nodes: int = DEFAULT_MAX_NODES,
    ) -> None:
        if dispatcher is None:
            raise ValueError("OscQueryService requires a Dispatcher instance")
        if max_nodes < 1:
            raise ValueError(f"max_nodes must be >= 1, got {max_nodes}")
        self._dispatcher = dispatcher
        self._host = host
        self._port = port
        self._max_nodes = max_nodes
        self._lock = threading.Lock()
        self._server: Optional[_OscQueryHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def build_tree(self) -> Dict[str, Any]:
        """Return the current address tree as a JSON-serializable dict.

        Raises:
            OscQueryTreeTooLargeError: If the tree exceeds max_nodes.
        """
        addresses, metadata = self._dispatcher.oscquery_snapshot()
        return build_address_tree(addresses, metadata, self._max_nodes)

    @property
    def is_running(self) -> bool:
        """Whether the HTTP service is currently serving."""
        return self._server is not None

    @property
    def port(self) -> int:
        """The bound TCP port. Raises OscQueryError if the service is stopped."""
        server = self._server
        if server is None:
            raise OscQueryError("OSCQuery service is not running")
        return int(server.server_address[1])

    @property
    def url(self) -> str:
        """Base URL of the service. Raises OscQueryError if it is stopped."""
        return f"http://{self._host}:{self.port}"

    def start(self) -> None:
        """Start the HTTP service on a background thread.

        Raises:
            OscQueryError: If the service is already running.
        """
        with self._lock:
            if self._server is not None:
                raise OscQueryError("OSCQuery service is already running")
            server = _OscQueryHTTPServer((self._host, self._port), self)
            thread = threading.Thread(
                target=server.serve_forever,
                name="oscquery-http",
                daemon=True,
            )
            thread.start()
            self._server = server
            self._thread = thread

    def stop(self) -> None:
        """Stop the service, release the port and join the server thread.

        Safe to call multiple times; stopping a stopped service is a no-op.
        """
        with self._lock:
            server, thread = self._server, self._thread
            self._server = None
            self._thread = None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)

    def __enter__(self) -> "OscQueryService":
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()
