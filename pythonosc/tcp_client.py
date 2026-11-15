"""TCP Clients for sending OSC messages to an OSC server"""

import asyncio
import socket
from contextlib import suppress
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Generator,
    Iterable,
    List,
    Optional,
    Union,
)

from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_bundle import OscBundle
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue, build_msg
from pythonosc.osc_tcp_server import MODE_1_1, get_framing
from pythonosc.parsing.framing import FramingParser
from pythonosc.send_queue import (
    BoundedSendQueue,
    SendQueueClosed,
    SendQueueFull,
    SendQueueFullPolicy,
)

__all__ = [
    "AsyncDispatchTCPClient",
    "AsyncSimpleTCPClient",
    "AsyncTCPClient",
    "BoundedSendQueue",
    "QueuedAsyncSimpleTCPClient",
    "QueuedAsyncTCPClient",
    "SendQueueClosed",
    "SendQueueFull",
    "SendQueueFullPolicy",
    "SimpleTCPClient",
    "TCPClient",
    "TCPDispatchClient",
]


class TCPClient:
    """OSC client to send :class:`OscMessage` or :class:`OscBundle` via TCP"""

    def __init__(
        self,
        address: str,
        port: int,
        family: socket.AddressFamily = socket.AF_INET,
        mode: str = MODE_1_1,
        timeout: float | None = 30.0,
    ) -> None:
        """Initialize client

        Args:
            address: IP address of server
            port: Port of server
            family: address family parameter (passed to socket.getaddrinfo)
            timeout: Default timeout in seconds for socket operations
        """

        self.address = address
        self.port = port
        self.family = family
        self._timeout = timeout
        self._framing = get_framing(mode)
        self._parser = FramingParser(self._framing)
        self.socket = socket.socket(self.family, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self.socket.connect((address, port))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send(self, content: Union[OscMessage, OscBundle]) -> None:
        """Sends an :class:`OscMessage` or :class:`OscBundle` via TCP

        Args:
            content: Message or bundle to be sent
        """
        self.socket.sendall(self._framing.encode(content.dgram))

    def receive(self, timeout: float | None = None) -> List[bytes]:
        messages = []
        effective_timeout = timeout if timeout is not None else self._timeout
        self.socket.settimeout(effective_timeout)

        with suppress(TimeoutError):
            while chunk := self.socket.recv(16384):
                messages.extend(self._parser.feed(chunk))

        return messages

    def close(self):
        self._parser.reset()
        self.socket.close()


class SimpleTCPClient(TCPClient):
    """Simple OSC client that automatically builds :class:`OscMessage` from arguments"""

    def send_message(
        self, address: str, value: Union[ArgValue, Iterable[ArgValue]] = ()
    ) -> None:
        """Build :class:`OscMessage` from arguments and send to server

        Args:
            address: OSC address the message shall go to
            value: One or more arguments to be added to the message. Omit for
                no arguments; an empty string is a string argument.
        """
        msg = build_msg(address, value)
        return self.send(msg)

    def get_messages(self, timeout: float | None = None) -> Generator:
        r = self.receive(timeout)
        while r:
            for m in r:
                yield OscMessage(m)
            r = self.receive(timeout)


class TCPDispatchClient(SimpleTCPClient):
    """OSC TCP Client that includes a :class:`Dispatcher` for handling responses and other messages from the server"""

    def __init__(self, *args: Any, dispatcher: Dispatcher | None = None, **kwargs: Any):
        self.dispatcher = dispatcher or Dispatcher()
        super().__init__(*args, **kwargs)

    def handle_messages(self, timeout_sec: float | None = None) -> None:
        """Wait :int:`timeout` seconds for a message from the server and process each message with the registered
        handlers.  Continue until a timeout occurs.

        Args:
            timeout: Time in seconds to wait for a message
        """
        r = self.receive(timeout_sec)
        while r:
            for m in r:
                self.dispatcher.call_handlers_for_packet(m, (self.address, self.port))
            r = self.receive(timeout_sec)


class AsyncTCPClient:
    """Async OSC client to send :class:`OscMessage` or :class:`OscBundle` via TCP"""

    def __init__(
        self,
        address: str,
        port: int,
        family: socket.AddressFamily = socket.AF_INET,
        mode: str = MODE_1_1,
        timeout: float | None = 30.0,
    ) -> None:
        """Initialize client

        Args:
            address: IP address of server
            port: Port of server
            family: address family parameter (passed to socket.getaddrinfo)
            timeout: Default timeout in seconds for socket operations
        """
        self.address: str = address
        self.port: int = port
        self.family: socket.AddressFamily = family
        self._timeout = timeout
        self._framing = get_framing(mode)
        self._parser = FramingParser(self._framing)

    async def __aenter__(self):
        await self.__open__()
        return self

    async def __open__(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.address, self.port
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def send(self, content: Union[OscMessage, OscBundle]) -> Awaitable[None]:
        """Sends an :class:`OscMessage` or :class:`OscBundle` via TCP

        Args:
            content: Message or bundle to be sent
        """
        self.writer.write(self._framing.encode(content.dgram))
        return self.writer.drain()

    def _read_with_timeout(self, timeout: float | None) -> Awaitable[bytes]:
        return asyncio.wait_for(self.reader.read(16384), timeout)

    async def receive(self, timeout: float | None = None) -> List[bytes]:
        messages = []
        effective_timeout = timeout if timeout is not None else self._timeout

        with suppress(TimeoutError):
            while chunk := await self._read_with_timeout(effective_timeout):
                messages.extend(self._parser.feed(chunk))

        return messages

    def close(self) -> Awaitable[None]:
        self._parser.reset()
        self.writer.write_eof()
        self.writer.close()
        return self.writer.wait_closed()


class AsyncSimpleTCPClient(AsyncTCPClient):
    """Simple OSC client that automatically builds :class:`OscMessage` from arguments"""

    async def send_message(
        self, address: str, value: Union[ArgValue, Iterable[ArgValue]] = ()
    ) -> None:
        """Build :class:`OscMessage` from arguments and send to server

        Args:
            address: OSC address the message shall go to
            value: One or more arguments to be added to the message. Omit for
                no arguments; an empty string is a string argument.
        """
        msg = build_msg(address, value)
        return await self.send(msg)

    async def get_messages(self, timeout: float | None = None) -> AsyncGenerator:
        r = await self.receive(timeout)
        while r:
            for m in r:
                yield OscMessage(m)
            r = await self.receive(timeout)


class AsyncDispatchTCPClient(AsyncTCPClient):
    """OSC Client that includes a :class:`Dispatcher` for handling responses and other messages from the server"""

    def __init__(self, *args: Any, dispatcher: Dispatcher | None = None, **kwargs: Any):
        self.dispatcher = dispatcher or Dispatcher()
        super().__init__(*args, **kwargs)

    async def handle_messages(self, timeout: float | None = None) -> None:
        """Wait :int:`timeout` seconds for a message from the server and process each message with the registered
        handlers.  Continue until a timeout occurs.

        Args:
            timeout: Time in seconds to wait for a message
        """
        msgs = await self.receive(timeout)
        while msgs:
            for m in msgs:
                await self.dispatcher.async_call_handlers_for_packet(
                    m, (self.address, self.port)
                )
            msgs = await self.receive(timeout)


class QueuedAsyncTCPClient(AsyncTCPClient):
    """Async OSC TCP client with a bounded, ordered background send queue.

    Messages are framed at enqueue time and handed to a single dedicated
    writer task that writes them strictly in enqueue order. Concurrent
    producers therefore cannot interleave or truncate frames, and a slow
    receiver applies backpressure through the queue bounds instead of
    accumulating unbounded write tasks.

    The queue can be bounded by message count (``max_messages``) and/or by
    the number of framed bytes pending (``max_bytes``). When both bounds are
    ``None`` the queue is unbounded.

    The direct-send API of :class:`AsyncTCPClient` is preserved; only
    :meth:`send` is overridden and now reports whether the message was sent.

    Args:
        address: IP address of server
        port: Port of server
        family: address family parameter (passed to socket.getaddrinfo)
        mode: OSC framing mode, ``"1.1"`` (SLIP) or ``"1.0"`` (length prefix)
        timeout: Default timeout in seconds for socket operations
        max_messages: Maximum number of queued messages, or ``None``.
        max_bytes: Maximum number of queued framed bytes, or ``None``.
        full_policy: Default behaviour when the queue is full; per-call
            overrides are possible on :meth:`send`.
        close_timeout: Upper bound in seconds for the transport to finish
            closing. ``None`` waits indefinitely.
    """

    #: Fallback when no explicit close timeout is given.
    DEFAULT_CLOSE_TIMEOUT = 5.0

    def __init__(
        self,
        address: str,
        port: int,
        family: socket.AddressFamily = socket.AF_INET,
        mode: str = MODE_1_1,
        timeout: float | None = 30.0,
        max_messages: Optional[int] = None,
        max_bytes: Optional[int] = None,
        full_policy: SendQueueFullPolicy = SendQueueFullPolicy.WAIT,
        close_timeout: Optional[float] = DEFAULT_CLOSE_TIMEOUT,
    ) -> None:
        super().__init__(address, port, family, mode, timeout)
        self._queue = BoundedSendQueue(max_messages=max_messages, max_bytes=max_bytes)
        self._default_full_policy = full_policy
        self._close_timeout = close_timeout
        self._sender_task: Optional[asyncio.Task[None]] = None
        self._closing = False

    async def __open__(self) -> None:
        await super().__open__()
        if self._sender_task is None:
            self._sender_task = asyncio.create_task(
                self._send_loop(), name="osc-tcp-send-loop"
            )

    async def send(  # type: ignore[override]
        self,
        content: Union[OscMessage, OscBundle],
        full_policy: Optional[SendQueueFullPolicy] = None,
    ) -> bool:
        """Queue one message or bundle and await its delivery.

        The complete OSC frame is encoded before enqueueing, so frames are
        never split or reordered.

        Args:
            content: Message or bundle to be sent.
            full_policy: Override the client default. ``WAIT`` blocks until
                capacity is available; ``ERROR`` raises
                :class:`~pythonosc.send_queue.SendQueueFull` immediately.

        Returns:
            ``True`` once the whole frame has been written to the transport.
            ``False`` if a discarding shutdown dropped the message before it
            was written.

        Raises:
            SendQueueFull: With the ``ERROR`` policy and a full queue.
            SendQueueClosed: The client was closed (or its connection failed)
                before the message was accepted.
            OSError: The connection broke before the frame could be written;
                the same error is delivered to every queued sender.
        """

        policy = full_policy if full_policy is not None else self._default_full_policy
        future = await self._queue.enqueue(
            self._framing.encode(content.dgram), full_policy=policy
        )
        return await future

    async def _send_loop(self) -> None:
        """Single consumer: writes every queued frame in strict FIFO order."""

        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                try:
                    # write() only buffers; drain() is the await point that
                    # applies transport backpressure and surfaces errors.
                    self.writer.write(item.frame)
                    await self.writer.drain()
                except BaseException as exc:  # noqa: BLE001 - propagated to all senders
                    # A cancelled writer (discarding shutdown or external
                    # cancellation) cannot complete the frame, and any other
                    # error means the connection is gone. Fail the queue so
                    # no producer is left waiting forever, then re-raise a
                    # cancellation so the task ends cleanly.
                    if not item.future.done():
                        if isinstance(exc, asyncio.CancelledError):
                            item.future.set_result(False)
                        else:
                            item.future.set_exception(exc)
                    await self._queue.fail(exc)
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    return
                else:
                    self._queue.mark_sent(item.future)
        except asyncio.CancelledError:
            raise

    async def close(self, drain: bool = True) -> None:
        """Shut the client down.

        Args:
            drain: When true (the default) all already queued messages are
                written in order before the connection is closed. When false
                queued messages are dropped immediately and their
                :meth:`send` calls resolve to ``False``.
        """

        if self._closing:
            return
        self._closing = True

        if self._sender_task is not None:
            await self._queue.close(drain=drain)
            if not drain:
                self._sender_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._sender_task
            self._sender_task = None

        await self._close_transport()

    async def _close_transport(self) -> None:
        self._parser.reset()
        with suppress(NotImplementedError, OSError):
            if self.writer.can_write_eof():
                self.writer.write_eof()
        self.writer.close()
        # wait_closed() can block indefinitely when the peer never reads the
        # in-flight data; the socket is already closing, so bound the wait and
        # tolerate resets or cancellations surfaced during teardown.
        with suppress(
            OSError,
            ConnectionError,
            asyncio.TimeoutError,
            asyncio.CancelledError,
        ):
            if self._close_timeout is None:
                await self.writer.wait_closed()
            else:
                await asyncio.wait_for(
                    self.writer.wait_closed(), timeout=self._close_timeout
                )


class QueuedAsyncSimpleTCPClient(QueuedAsyncTCPClient):
    """Queued async OSC client that builds :class:`OscMessage` from arguments"""

    async def send_message(
        self,
        address: str,
        value: Union[ArgValue, Iterable[ArgValue]] = (),
        full_policy: Optional[SendQueueFullPolicy] = None,
    ) -> bool:
        """Build an :class:`OscMessage` from arguments and queue it.

        Args:
            address: OSC address the message shall go to
            value: One or more arguments to be added to the message. Omit for
                no arguments; an empty string is a string argument.
            full_policy: Override the client-wide full-queue policy.

        Returns:
            ``True`` when the frame was written, ``False`` if it was
            discarded by a discarding shutdown.
        """
        msg = build_msg(address, value)
        return await self.send(msg, full_policy=full_policy)
