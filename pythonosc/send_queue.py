"""Bounded, ordered send queue for the async OSC TCP client.

A single dedicated writer task pulls framed OSC packets from the queue and
writes them to the TCP stream in enqueue order, so concurrent producers can
never interleave or truncate a frame.  The queue is bounded both by the number
of pending messages and by the number of pending bytes.
"""

import asyncio
import logging
from collections import deque
from enum import Enum
from typing import NamedTuple, Optional

logger = logging.getLogger(__name__)


class SendQueueFullPolicy(Enum):
    """What ``enqueue`` does when the bounded queue has no capacity.

    * ``WAIT``: wait asynchronously until capacity becomes available, the
      queue is closed or the awaiting task is cancelled.
    * ``ERROR``: raise :class:`SendQueueFull` immediately instead of waiting.
    """

    WAIT = "wait"
    ERROR = "error"


class SendQueueError(Exception):
    """Base class for errors raised by the bounded send queue."""


class SendQueueFull(SendQueueError):
    """Raised when an ``ERROR`` policy enqueue finds the queue full."""


class SendQueueClosed(SendQueueError):
    """Raised when enqueuing into a closed or failed queue."""


class _QueueItem(NamedTuple):
    frame: bytes
    future: "asyncio.Future[bool]"


class BoundedSendQueue:
    """FIFO queue of framed packets with message-count and byte bounds.

    The queue itself is transport agnostic: a single consumer calls
    :meth:`get` and reports the outcome with :meth:`mark_sent` or
    :meth:`fail`.  Any number of producers may call :meth:`enqueue`
    concurrently; a condition variable serialises access to the internal
    state so wake-ups are not lost.

    Args:
        max_messages: Maximum number of pending messages. ``None`` means
            unlimited.
        max_bytes: Maximum number of pending (framed) bytes. ``None`` means
            unlimited.
    """

    def __init__(
        self,
        max_messages: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> None:
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be positive or None")
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("max_bytes must be positive or None")
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._items: deque[_QueueItem] = deque()
        self._pending_bytes = 0
        self._condition = asyncio.Condition()
        self._closed = False
        self._error: Optional[BaseException] = None

    @property
    def closed(self) -> bool:
        """Whether the queue stops accepting new messages."""

        return self._closed

    def qsize(self) -> int:
        """Number of messages waiting to be written."""

        return len(self._items)

    def size_bytes(self) -> int:
        """Number of framed bytes waiting to be written."""

        return self._pending_bytes

    def _is_full(self, frame_size: int) -> bool:
        if self._max_messages is not None and len(self._items) >= self._max_messages:
            return True
        return (
            self._max_bytes is not None
            and self._pending_bytes + frame_size > self._max_bytes
        )

    async def enqueue(
        self,
        frame: bytes,
        full_policy: SendQueueFullPolicy = SendQueueFullPolicy.WAIT,
    ) -> "asyncio.Future[bool]":
        """Append one framed packet and return a future tracking its delivery.

        The future resolves to ``True`` once the whole frame has been handed
        to the TCP transport, resolves to ``False`` when the message is
        discarded by a discarding shutdown, or carries the connection error
        that prevented it from being sent.

        Args:
            frame: The already framed bytes of one complete OSC packet.
            full_policy: Whether to wait for capacity or raise
                :class:`SendQueueFull` immediately.

        Raises:
            SendQueueFull: With the ``ERROR`` policy and no capacity.
            SendQueueClosed: The queue has been closed or has failed.
            ValueError: A single frame exceeds ``max_bytes`` and could never
                fit.
        """

        frame = bytes(frame)
        loop = asyncio.get_running_loop()
        async with self._condition:
            if self._max_bytes is not None and len(frame) > self._max_bytes:
                raise ValueError(
                    f"frame of {len(frame)} bytes exceeds max_bytes "
                    f"of {self._max_bytes}"
                )

            if full_policy is SendQueueFullPolicy.ERROR and self._is_full(len(frame)):
                raise SendQueueFull(
                    f"send queue full: {len(self._items)} messages, "
                    f"{self._pending_bytes} bytes"
                )

            await self._condition.wait_for(
                lambda: (
                    self._closed
                    or self._error is not None
                    or not self._is_full(len(frame))
                )
            )

            if self._error is not None:
                raise SendQueueClosed("send queue has failed") from self._error
            if self._closed:
                raise SendQueueClosed("send queue is closed")

            future: asyncio.Future[bool] = loop.create_future()
            future.add_done_callback(_log_if_exception_unretrieved)
            self._items.append(_QueueItem(frame, future))
            self._pending_bytes += len(frame)
            # Wake the single writer; producers are woken by get/fail/close.
            self._condition.notify(1)
            return future

    async def get(self) -> Optional[_QueueItem]:
        """Remove and return the next item, or ``None`` when drained/closed.

        Waits while the queue is empty. Returns ``None`` once the queue is
        closed or has failed and no items remain, giving the writer a clean
        way to terminate.
        """

        async with self._condition:
            await self._condition.wait_for(
                lambda: bool(self._items) or self._closed or self._error is not None
            )
            if not self._items:
                return None
            item = self._items.popleft()
            self._pending_bytes -= len(item.frame)
            # Capacity freed: wake every producer that may be waiting.
            self._condition.notify_all()
            return item

    def mark_sent(self, future: "asyncio.Future[bool]") -> None:
        """Resolve an item's future after its frame was fully written."""

        if not future.done():
            future.set_result(True)

    async def fail(self, error: BaseException) -> None:
        """Permanently fail the queue after a connection error.

        Every still-queued message receives ``error`` on its future and
        subsequent enqueues raise :class:`SendQueueClosed`.
        """

        async with self._condition:
            for item in self._items:
                if not item.future.done():
                    item.future.set_exception(error)
            self._items.clear()
            self._pending_bytes = 0
            self._error = error
            self._closed = True
            self._condition.notify_all()

    async def close(self, *, drain: bool = True) -> None:
        """Stop accepting new messages.

        Args:
            drain: When true (the default) already queued messages stay
                available to :meth:`get` so the writer can flush them. When
                false all queued messages are discarded immediately and
                their futures resolve to ``False``.
        """

        async with self._condition:
            self._closed = True
            if not drain:
                for item in self._items:
                    if not item.future.done():
                        item.future.set_result(False)
                self._items.clear()
                self._pending_bytes = 0
            self._condition.notify_all()


def _log_if_exception_unretrieved(future: "asyncio.Future[bool]") -> None:
    """Avoid "exception was never retrieved" warnings for abandoned sends."""

    if future.cancelled():
        return
    exc = future.exception()
    if exc is not None:
        logger.debug("unretrieved send queue future exception: %r", exc)
