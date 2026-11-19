"""Per-address rate limiting for :class:`~pythonosc.dispatcher.Dispatcher` routes.

A :class:`RouteRateLimiter` combines a token bucket (the *rate* and *burst*
configured on ``Dispatcher.map``) with a bounded FIFO queue of messages that
could not be delivered immediately.  Two overflow strategies are provided:

* :attr:`OverflowPolicy.DROP_OLDEST` keeps the newest value: when the queue is
  full the oldest queued message is discarded to make room for the new one.
  This matches the "high frequency knob stream" use case where only the latest
  position matters.
* :attr:`OverflowPolicy.REJECT_NEW` refuses the new message instead, leaving
  the already queued messages untouched.

The limiter is clock-agnostic: every time read goes through an injected
``clock`` callable so tests can drive it with a virtual clock.  Clock
regressions (non-monotonic time) are handled by treating the elapsed time as
zero, so a backwards jump can never generate free tokens.
"""

from __future__ import annotations

import collections
import enum
import threading
from dataclasses import dataclass
from typing import Any, Callable, Deque, List, Optional, Tuple

from pythonosc.osc_message import OscMessage


class RateLimitError(ValueError):
    """Raised when rate limiting parameters are invalid."""


class OverflowPolicy(enum.Enum):
    """What a :class:`RouteRateLimiter` does when its queue is full.

    Attributes:
        DROP_OLDEST: Discard the oldest queued message and enqueue the new one
            (latest value wins).
        REJECT_NEW: Reject the new message, keep everything already queued.
    """

    DROP_OLDEST = "drop_oldest"
    REJECT_NEW = "reject_new"


class SubmitResult(enum.Enum):
    """Outcome of :meth:`RouteRateLimiter.submit`."""

    DELIVERED = "delivered"
    QUEUED = "queued"
    REJECTED = "rejected"


@dataclass
class RateLimitStats:
    """Counters for a single route's limiter."""

    submitted: int = 0
    delivered: int = 0
    queued: int = 0
    rejected: int = 0
    dropped_old: int = 0

    @property
    def dropped(self) -> int:
        """Total number of messages that never reached a handler."""
        return self.rejected + self.dropped_old

    @property
    def accounted(self) -> int:
        """Messages currently delivered or still held in the queue."""
        return self.delivered + self.queued


class TokenBucket:
    """A refilling token bucket.

    Tokens accumulate at ``rate`` tokens per second, capped at ``burst``.  Each
    :meth:`take` consumes one token if any is available.  Negative elapsed time
    (clock going backwards) clamps to zero elapsed time instead of refilling
    the bucket.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        clock: Callable[[], float],
        *,
        initial_tokens: Optional[float] = None,
    ) -> None:
        if rate <= 0:
            raise RateLimitError(f"rate must be > 0, got {rate!r}")
        if burst <= 0:
            raise RateLimitError(f"burst must be > 0, got {burst!r}")
        self._rate = float(rate)
        self._burst = float(burst)
        self._clock = clock
        self._tokens = float(burst if initial_tokens is None else initial_tokens)
        self._last_refill = clock()
        self._lock = threading.Lock()

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def burst(self) -> float:
        return self._burst

    @property
    def tokens(self) -> float:
        """Currently available tokens after a refill."""
        with self._lock:
            self._refill()
            return self._tokens

    def _refill(self) -> None:
        """Add the tokens accrued since the last refill.

        Must be called with ``self._lock`` held.
        """
        now = self._clock()
        elapsed = now - self._last_refill
        if elapsed < 0:
            # Clock moved backwards: do not grant tokens for the jump, just
            # re-anchor the refill time at the new reading.
            elapsed = 0.0
        self._last_refill = now
        if elapsed > 0:
            self._tokens = min(
                self._burst, self._tokens + elapsed * self._rate
            )

    def take(self) -> bool:
        """Try to consume one token.

        Returns:
            True if a token was available and consumed, False otherwise.
        """
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def refund(self) -> None:
        """Give back one token (capped at the burst size).

        Used when a delivery has to be undone because another matched route
        rejected the same message.
        """
        with self._lock:
            self._refill()
            self._tokens = min(self._burst, self._tokens + 1.0)

    def time_until_token(self) -> float:
        """Seconds until at least one full token is available (>= 0)."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                return 0.0
            return (1.0 - self._tokens) / self._rate

    def reconfigure(self, rate: float, burst: float) -> None:
        """Change the rate/burst, preserving pro-rated accrued tokens."""
        if rate <= 0:
            raise RateLimitError(f"rate must be > 0, got {rate!r}")
        if burst <= 0:
            raise RateLimitError(f"burst must be > 0, got {burst!r}")
        with self._lock:
            self._refill()
            self._rate = float(rate)
            self._burst = float(burst)
            self._tokens = min(self._tokens, self._burst)


@dataclass
class QueuedMessage:
    """A message waiting for a token on a rate limited route."""

    client_address: Tuple[str, int]
    message: OscMessage
    time: float
    handlers: List[Any]
    # Set once the token is consumed, before the handlers are invoked.
    ready: bool = False


class RouteRateLimiter:
    """Token bucket plus bounded queue for one mapped address pattern.

    Messages submitted while a token is available are delivered immediately.
    Otherwise they are queued (FIFO) up to ``queue_capacity`` messages; an
    overflowing queue follows the configured :class:`OverflowPolicy`.

    The object is thread-safe: a :class:`~threading.RLock` guards every
    mutation, which also lets :meth:`pump_due` run from a background thread
    while :meth:`submit` is called from server threads.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        clock: Callable[[], float],
        overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
        queue_capacity: Optional[int] = None,
    ) -> None:
        if rate <= 0:
            raise RateLimitError(f"rate must be > 0, got {rate!r}")
        if burst <= 0:
            raise RateLimitError(f"burst must be > 0, got {burst!r}")
        capacity = int(burst) if queue_capacity is None else queue_capacity
        if capacity < 1:
            raise RateLimitError(
                f"queue capacity must be >= 1, got {capacity!r}"
            )
        self._bucket = TokenBucket(rate, burst, clock)
        self._clock = clock
        self._overflow_policy = overflow_policy
        self._capacity = capacity
        self._queue: Deque[QueuedMessage] = collections.deque()
        # Entries evicted by DROP_OLDEST, kept briefly so a cross-route
        # rollback can reinstate the exact message that lost its slot. Bounded
        # to the queue capacity: rollback only ever needs the most recent
        # eviction of a still-pending submission.
        self._recent_evictions: Deque[QueuedMessage] = collections.deque(
            maxlen=capacity
        )
        self._stats = RateLimitStats()
        self._closed = False
        self._lock = threading.RLock()

    @property
    def stats(self) -> RateLimitStats:
        return self._stats

    @property
    def queue_size(self) -> int:
        with self._lock:
            return len(self._queue)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def overflow_policy(self) -> OverflowPolicy:
        return self._overflow_policy

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def reconfigure(
        self,
        rate: float,
        burst: float,
        overflow_policy: Optional[OverflowPolicy] = None,
    ) -> None:
        """Update rate/burst (and optionally policy), keeping the queue."""
        with self._lock:
            self._bucket.reconfigure(rate, burst)
            new_capacity = int(burst)
            self._capacity = new_capacity
            if overflow_policy is not None:
                self._overflow_policy = overflow_policy
            # Resize the rollback history to match the new capacity.
            resized: Deque[QueuedMessage] = collections.deque(
                self._recent_evictions, maxlen=new_capacity
            )
            self._recent_evictions = resized
            # Shrink the queue deterministically, oldest messages first.
            while len(self._queue) > self._capacity:
                dropped = self._queue.popleft()
                self._stats.dropped_old += 1
                self._stats.queued -= 1
                del dropped

    def close(self) -> int:
        """Close the limiter and drop everything still queued.

        Returns:
            Number of queued messages that were dropped.
        """
        with self._lock:
            dropped = len(self._queue)
            self._queue.clear()
            self._recent_evictions.clear()
            self._stats.queued = 0
            self._closed = True
            return dropped

    def submit(
        self,
        client_address: Tuple[str, int],
        message: OscMessage,
        time: float,
        handlers: List[Any],
        honor_timetag: bool = True,
    ) -> SubmitResult:
        """Admit a message matched to this route.

        Args:
            client_address: Originating client address.
            message: The OSC message.
            time: The message's effective dispatch time (timetag for bundled
                messages, arrival time otherwise).
            handlers: Handlers to invoke when the message is (eventually)
                delivered.
            honor_timetag: When False, future timetags do not force queueing;
                only token availability matters (used when the owning
                dispatcher runs with ``strict_timing=False``).

        Returns:
            A :class:`SubmitResult` describing what happened.
        """
        with self._lock:
            self._stats.submitted += 1
            if self._closed:
                self._stats.rejected += 1
                return SubmitResult.REJECTED
            # A message may be delivered inline only when it has both a token
            # and, when timetags are honored, a timetag that has already
            # arrived. A future-dated message queues without consuming a token
            # so the bundle timetag is respected regardless of capacity.
            timetag_due = (not honor_timetag) or time <= self._clock()
            if timetag_due and self._bucket.take():
                self._stats.delivered += 1
                return SubmitResult.DELIVERED
            # No token left (or future timetag): the message must wait in the
            # bounded queue.
            if len(self._queue) >= self._capacity:
                if self._overflow_policy is OverflowPolicy.REJECT_NEW:
                    self._stats.rejected += 1
                    return SubmitResult.REJECTED
                # DROP_OLDEST: latest value wins. Remember the evicted entry so
                # a later rollback_submission() can reinstate it verbatim.
                evicted = self._queue.popleft()
                self._recent_evictions.append(evicted)
                self._stats.dropped_old += 1
                self._stats.queued -= 1
            entry = QueuedMessage(
                client_address, message, time, list(handlers)
            )
            self._queue.append(entry)
            self._stats.queued += 1
            return SubmitResult.QUEUED

    def rollback_submission(
        self, message: OscMessage, decision: SubmitResult
    ) -> None:
        """Undo a previous :meth:`submit` on behalf of ``message``.

        Needed when one of several matched routes rejects a message: every
        other route must restore the state it consumed so the rejected
        message has no side effects anywhere. A queued submission that had
        evicted an older message (DROP_OLDEST) reinstates that exact entry at
        the front of the queue.

        Args:
            message: The message whose submission is rolled back.
            decision: The result originally returned by :meth:`submit`.
        """
        with self._lock:
            if decision is SubmitResult.DELIVERED:
                self._bucket.refund()
                self._stats.delivered -= 1
                return
            if decision is not SubmitResult.QUEUED:
                return
            # Remove this route's own entry (the newest matching one).
            target: Optional[QueuedMessage] = None
            for index in range(len(self._queue) - 1, -1, -1):
                if self._queue[index].message is message:
                    target = self._queue[index]
                    del self._queue[index]
                    self._stats.queued -= 1
                    break
            if target is None:
                # Already pumped (e.g. a manual drain between submit and
                # rollback): refund the token instead.
                self._bucket.refund()
                self._stats.delivered -= 1
                return
            # Undo the DROP_OLDEST eviction this submission caused, if any:
            # put the original entry back at its old front position.
            if self._recent_evictions:
                reinstated = self._recent_evictions.pop()
                self._queue.appendleft(reinstated)
                self._stats.dropped_old -= 1
                self._stats.queued += 1

    def pump_due(self, honor_timetag: bool = True) -> List[QueuedMessage]:
        """Mark queued messages that can be delivered now as ready.

        A message is only released when it has both a token and, when
        ``honor_timetag`` is True, a timetag that has already arrived. The
        queue is FIFO ordered by arrival time (and bundle contents are
        timetag-sorted before submission), so only the head needs inspecting:
        a future-timetag head blocks everything behind it and its token is
        left unconsumed for when it becomes due.

        Args:
            honor_timetag: When False, timetags are ignored and only token
                availability gates release (used when the dispatcher runs with
                ``strict_timing=False``).

        Returns:
            The messages released on this call, oldest first.
        """
        due: List[QueuedMessage] = []
        with self._lock:
            now = self._clock()
            while self._queue and (
                not honor_timetag or self._queue[0].time <= now
            ):
                if not self._bucket.take():
                    break
                item = self._queue.popleft()
                item.ready = True
                due.append(item)
                self._stats.queued -= 1
            for item in due:
                self._stats.delivered += 1
        return due

    def next_wakeup(
        self, honor_timetag: bool = True
    ) -> Optional[float]:
        """Absolute time of the next queued message's earliest delivery.

        This is the later of the head message's timetag and the moment its
        rate limit token will be available. When ``honor_timetag`` is False the
        timetag component is ignored.
        """
        with self._lock:
            if not self._queue:
                return None
            token_time = self._clock() + self._bucket.time_until_token()
            if not honor_timetag:
                return token_time
            return max(token_time, self._queue[0].time)

    def drain_all(self) -> List[QueuedMessage]:
        """Remove and return every queued message without consuming tokens.

        Used by :meth:`~pythonosc.dispatcher.Dispatcher.shutdown` with
        ``flush=True`` so queued messages are delivered instead of dropped.
        """
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
            self._stats.queued = 0
            for item in items:
                item.ready = True
            return items
