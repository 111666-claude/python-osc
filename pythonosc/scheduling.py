"""Bounded, non-blocking scheduling of OSC messages with future timetags.

The strict-timing mode built into :class:`~pythonosc.dispatcher.Dispatcher`
sleeps on the receive path until each timetag is due.  A lighting console that
sends thousands of future-dated messages in a single burst would therefore
block the processing path (or, with the threading server, spawn an unbounded
number of sleeping threads).

The schedulers in this module take a different approach: messages are enqueued
immediately and their callbacks are invoked later, when an injectable clock
reaches the timetag.  Pending events can be cancelled individually or all at
once.  Two implementations are provided:

* :class:`ThreadEventScheduler` for the synchronous servers (UDP/TCP).  One
  scheduler thread waits on the clock while a bounded worker pool runs the
  callbacks, so slow callbacks never block reception and the number of threads
  stays bounded.
* :class:`AsyncIOEventScheduler` for the asyncio servers.  Everything runs on
  the event loop, no extra threads are created.

Both schedulers share the same policies and observability surface:

* a fixed :class:`queue capacity <OverflowPolicy>`,
* configurable behaviour when that capacity is exhausted,
* configurable behaviour for events still pending when the scheduler closes
  (:class:`ClosePolicy`),
* a :class:`SchedulerStats` snapshot plus per-event listener callbacks.
"""

from __future__ import annotations

import asyncio
import enum
import heapq
import itertools
import logging
import queue
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "AsyncIOEventScheduler",
    "ClosePolicy",
    "EventScheduler",
    "EventState",
    "OverflowPolicy",
    "QueueFullError",
    "ScheduledEvent",
    "SchedulerClosedError",
    "SchedulerError",
    "SchedulerStats",
    "ThreadEventScheduler",
]

_logger = logging.getLogger(__name__)

# Signature of the callable run when an event becomes due.
Callback = Callable[[], Any]
# Signature of the optional listener notified on every lifecycle transition.
Listener = Callable[["ScheduledEvent"], None]


class OverflowPolicy(enum.Enum):
    """What happens when a bounded scheduler queue is full.

    * ``REJECT``: refuse the new event.  :meth:`EventScheduler.schedule`
      returns ``None``, the ``rejected`` counter is increased and the listener
      is notified with :attr:`EventState.REJECTED`.
    * ``RAISE``: raise :class:`SchedulerClosedError`-style
      :class:`QueueFullError` instead of silently dropping anything.
    * ``DROP_OLDEST``: cancel the oldest pending event (it is reported through
      the listener as :attr:`EventState.DROPPED`) and enqueue the new one.
    * ``DROP_NEWEST``: silently discard the new event, exactly like ``REJECT``
      but without notifying the listener.
    """

    REJECT = "reject"
    RAISE = "raise"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class ClosePolicy(enum.Enum):
    """What happens to events still pending when a scheduler is closed.

    * ``CANCEL_PENDING``: pending events are cancelled and never run.
    * ``FLUSH``: every pending event is run as soon as possible, ignoring its
      timetag, before :meth:`EventScheduler.close` returns.
    * ``WAIT_PENDING``: the scheduler keeps running until all pending events
      have fired at their scheduled time (subject to the close timeout).
    """

    CANCEL_PENDING = "cancel_pending"
    FLUSH = "flush"
    WAIT_PENDING = "wait_pending"


class EventState(enum.Enum):
    """Lifecycle state of a :class:`ScheduledEvent`."""

    PENDING = "pending"
    FIRED = "fired"
    CANCELLED = "cancelled"
    DROPPED = "dropped"
    REJECTED = "rejected"


class SchedulerError(Exception):
    """Base class for scheduler errors."""


class SchedulerClosedError(SchedulerError):
    """Raised when scheduling on a closed scheduler."""


class QueueFullError(SchedulerError):
    """Raised on overflow with :attr:`OverflowPolicy.RAISE`."""


@dataclass
class ScheduledEvent:
    """A handle on one scheduled callback.

    Handles are returned by :meth:`EventScheduler.schedule` (except when the
    event was refused) and can be used to cancel the event before it fires.
    """

    time: float
    callback: Callback
    sequence: int
    state: EventState = field(default=EventState.PENDING)
    error: BaseException | None = field(default=None)

    def __lt__(self, other: ScheduledEvent) -> bool:
        # Order the heap by (time, sequence), never by the callback.
        return (self.time, self.sequence) < (other.time, other.sequence)

    @property
    def pending(self) -> bool:
        """True while the event has neither fired nor been cancelled."""
        return self.state is EventState.PENDING

    def cancel(self) -> bool:
        """Mark the event cancelled.

        Returns:
            True if the event was pending and is now cancelled, False if it had
            already fired or been cancelled.
        """
        if self.state is EventState.PENDING:
            self.state = EventState.CANCELLED
            return True
        return False


@dataclass
class SchedulerStats:
    """Immutable-ish snapshot of scheduler counters and queue depth."""

    capacity: int | None
    pending: int
    scheduled: int
    fired: int
    cancelled: int
    dropped: int
    rejected: int
    errors: int
    closed: bool

    def __str__(self) -> str:
        return (
            f"SchedulerStats(pending={self.pending}/{self.capacity}, "
            f"scheduled={self.scheduled}, fired={self.fired}, "
            f"cancelled={self.cancelled}, dropped={self.dropped}, "
            f"rejected={self.rejected}, errors={self.errors})"
        )


class EventScheduler:
    """Common, transport-agnostic bounded scheduling logic.

    Subclasses provide the waiting and callback-running primitives.  Events are
    kept in a min-heap ordered by ``(time, sequence)`` so the earliest timetag
    fires first and events sharing a timetag keep the order in which they were
    received (this is what preserves the original ordering inside an OSC
    bundle).
    """

    def __init__(
        self,
        capacity: int | None = 10000,
        overflow_policy: OverflowPolicy = OverflowPolicy.REJECT,
        close_policy: ClosePolicy = ClosePolicy.CANCEL_PENDING,
        clock: Callable[[], float] | None = None,
        listener: Listener | None = None,
    ) -> None:
        """Initialize the scheduler.

        Args:
            capacity: Maximum number of pending events. ``None`` means
                unbounded.
            overflow_policy: Behaviour once ``capacity`` is reached.
            close_policy: Behaviour for pending events on :meth:`close`.
            clock: Zero-argument callable returning the current time in
                seconds.  Defaults to :func:`time.time`; inject a fake clock in
                tests to make scheduling deterministic.
            listener: Optional callable invoked with every
                :class:`ScheduledEvent` when it changes state.
        """
        if capacity is not None and capacity <= 0:
            raise ValueError("capacity must be a positive integer or None")
        self._capacity = capacity
        self._overflow_policy = overflow_policy
        self._close_policy = close_policy
        self._clock = clock or time.time
        self._listener = listener
        self._heap: list[ScheduledEvent] = []
        self._counter = itertools.count()
        self._closed = False
        # Counters are mutated by subclasses under their own synchronization.
        self._scheduled_count = 0
        self._fired_count = 0
        self._cancelled_count = 0
        self._dropped_count = 0
        self._rejected_count = 0
        self._error_count = 0

    @property
    def capacity(self) -> int | None:
        """Maximum number of pending events, or ``None`` if unbounded."""
        return self._capacity

    @property
    def closed(self) -> bool:
        """Whether the scheduler has been closed."""
        return self._closed

    def now(self) -> float:
        """Return the scheduler's current time according to its clock."""
        return self._clock()

    def stats(self) -> SchedulerStats:
        """Return a snapshot of counters and current queue depth."""
        return SchedulerStats(
            capacity=self._capacity,
            pending=sum(1 for event in self._heap if event.pending),
            scheduled=self._scheduled_count,
            fired=self._fired_count,
            cancelled=self._cancelled_count,
            dropped=self._dropped_count,
            rejected=self._rejected_count,
            errors=self._error_count,
            closed=self._closed,
        )

    def pending_events(self) -> Sequence[ScheduledEvent]:
        """Return the pending events in firing order (snapshot)."""
        return sorted(
            (event for event in self._heap if event.pending),
            key=lambda event: (event.time, event.sequence),
        )

    # -- internal helpers, called under the subclass synchronization --------

    def _prune_cancelled(self) -> None:
        """Remove events that were cancelled through their handles.

        Counters and listeners are updated here, at the single point where a
        cancellation becomes observable, so :meth:`cancel` / :meth:`cancel_all`
        never double-count events they only marked.
        """
        if not any(not event.pending for event in self._heap):
            return
        removed = [event for event in self._heap if not event.pending]
        self._heap = [event for event in self._heap if event.pending]
        heapq.heapify(self._heap)
        for event in removed:
            if event.state is EventState.CANCELLED:
                self._cancelled_count += 1
                self._notify(event)

    def _peek_due(self) -> ScheduledEvent | None:
        self._prune_cancelled()
        if not self._heap:
            return None
        event = self._heap[0]
        if event.time <= self._clock():
            return event
        return None

    def _pop_due_events(self) -> list[ScheduledEvent]:
        """Pop every event that is due now, grouped and ordered correctly."""
        due: list[ScheduledEvent] = []
        while True:
            event = self._peek_due()
            if event is None:
                break
            heapq.heappop(self._heap)
            due.append(event)
        return due

    def _enqueue_locked(self, when: float, callback: Callback) -> ScheduledEvent:
        """Create and insert an event, enforcing the capacity policy.

        Raises:
            SchedulerClosedError: if the scheduler is closed.
            QueueFullError: on overflow with :attr:`OverflowPolicy.RAISE`.
        """
        if self._closed:
            raise SchedulerClosedError("cannot schedule on a closed scheduler")
        if self._capacity is not None and len(self._heap) >= self._capacity:
            if self._overflow_policy is OverflowPolicy.RAISE:
                raise QueueFullError(
                    f"scheduler queue is full (capacity={self._capacity})"
                )
            if self._overflow_policy is OverflowPolicy.DROP_OLDEST:
                self._prune_cancelled()
                if len(self._heap) >= self._capacity:
                    victim = heapq.heappop(self._heap)
                    victim.state = EventState.DROPPED
                    self._dropped_count += 1
                    self._notify(victim)
            else:
                # REJECT and DROP_NEWEST both discard the incoming event.
                rejected = ScheduledEvent(
                    when, callback, next(self._counter), EventState.REJECTED
                )
                self._rejected_count += 1
                if self._overflow_policy is OverflowPolicy.REJECT:
                    self._notify(rejected)
                else:
                    _logger.warning("scheduler queue full, dropping new event")
                return rejected
        event = ScheduledEvent(when, callback, next(self._counter))
        heapq.heappush(self._heap, event)
        self._scheduled_count += 1
        return event

    def _notify(self, event: ScheduledEvent) -> None:
        if self._listener is not None:
            try:
                self._listener(event)
            except Exception:  # pragma: no cover - listener errors must not leak
                _logger.exception("scheduler listener raised an exception")

    def _record_outcome(self, event: ScheduledEvent) -> None:
        if event.state is EventState.FIRED:
            self._fired_count += 1
        elif event.state is EventState.CANCELLED:
            self._cancelled_count += 1
        self._notify(event)

    def cancel(self, event: ScheduledEvent) -> bool:
        """Cancel a single event previously returned by :meth:`schedule`.

        The event stays in the heap until its slot is reached, then it is
        discarded and reported to the listener.

        Returns:
            True if the event was pending and got cancelled.
        """
        raise NotImplementedError

    def cancel_all(self) -> int:
        """Cancel every currently pending event.

        Returns:
            The number of events cancelled.
        """
        raise NotImplementedError

    def schedule(self, when: float, callback: Callback) -> ScheduledEvent | None:
        """Schedule ``callback`` to run at time ``when``.

        Events whose time is already in the past run as soon as possible.

        Returns:
            A :class:`ScheduledEvent` handle, or ``None`` if the event was
            refused by the overflow policy.
        """
        raise NotImplementedError

    def close(self, timeout: float | None = None) -> int:
        """Shut the scheduler down (synchronous schedulers only).

        Returns:
            The number of events that remained unexecuted.
        """
        raise NotImplementedError

    async def aclose(self, timeout: float | None = None) -> int:
        """Shut the scheduler down (asyncio scheduler only)."""
        raise NotImplementedError


class _BoundedWorkerPool:
    """A fixed-size thread pool with a bounded work queue.

    Unlike :class:`concurrent.futures.ThreadPoolExecutor` the queue is bounded
    so a flood of slow callbacks cannot grow memory without limit; once the
    queue is full the scheduler thread runs the job itself, which applies
    back-pressure on dispatch rather than spawning new threads.
    """

    def __init__(self, max_workers: int) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._max_workers = max_workers
        self._jobs: queue.Queue[Callback | None] = queue.Queue(
            maxsize=max_workers * 4 or 1
        )
        self._threads: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            for i in range(self._max_workers):
                thread = threading.Thread(
                    target=self._worker, name=f"osc-callback-{i}", daemon=True
                )
                thread.start()
                self._threads.append(thread)

    def _worker(self) -> None:
        while True:
            job = self._jobs.get()
            try:
                if job is None:
                    return
                job()
            finally:
                self._jobs.task_done()

    def submit(self, job: Callback) -> None:
        """Submit a job, running it on the calling thread if the queue is full."""
        if not self._started:
            # Manual mode (auto_start=False): no worker threads exist, so the
            # caller (typically a test driving process_due()) runs the job.
            job()
            return
        try:
            self._jobs.put(job, block=False)
        except queue.Full:
            # Designed back-pressure: all workers are busy, so the scheduler
            # thread runs this job itself instead of growing the thread count.
            _logger.debug("callback queue full, running on scheduler thread")
            job()

    def shutdown(self, timeout: float | None) -> None:
        if not self._started:
            return
        for _ in self._threads:
            self._jobs.put(None)
        for thread in self._threads:
            thread.join(timeout)


class ThreadEventScheduler(EventScheduler):
    """Bounded scheduler for the synchronous OSC servers.

    A single daemon thread sleeps until the next timetag and hands due
    callbacks to a bounded worker pool.  Receiving threads therefore never
    wait on a timetag and the total number of threads is
    ``1 + max_workers`` regardless of traffic.
    """

    def __init__(
        self,
        capacity: int | None = 10000,
        overflow_policy: OverflowPolicy = OverflowPolicy.REJECT,
        close_policy: ClosePolicy = ClosePolicy.CANCEL_PENDING,
        clock: Callable[[], float] | None = None,
        listener: Listener | None = None,
        max_workers: int = 1,
        auto_start: bool = True,
    ) -> None:
        """Initialize.

        Args:
            capacity: Maximum number of pending events (``None`` = unbounded).
            overflow_policy: Behaviour once the queue is full.
            close_policy: Behaviour for pending events on :meth:`close`.
            clock: Injectable time source, defaults to :func:`time.time`.
            listener: Optional state-change listener.
            max_workers: Size of the bounded pool running due callbacks.  With
                the default of one, callbacks fire strictly in timetag order;
                larger values allow parallel execution of different timetags.
            auto_start: When True (default), the pump thread and worker pool
                start on the first :meth:`schedule` call.  Set False with an
                injected clock to drive dispatch manually via
                :meth:`process_due` in tests.
        """
        super().__init__(capacity, overflow_policy, close_policy, clock, listener)
        self._condition = threading.Condition()
        self._workers = _BoundedWorkerPool(max_workers)
        self._thread: threading.Thread | None = None
        self._wakeup = False
        self._auto_start = auto_start

    def start(self) -> None:
        """Start the pump thread and worker pool explicitly."""
        with self._condition:
            if self._closed:
                raise SchedulerClosedError("cannot start a closed scheduler")
            self._start_locked()

    # -- public API ---------------------------------------------------------

    def schedule(self, when: float, callback: Callback) -> ScheduledEvent | None:
        with self._condition:
            event = self._enqueue_locked(when, callback)
            if event.state is not EventState.PENDING:
                return None
            self._wakeup = True
            self._condition.notify_all()
            if self._auto_start:
                self._start_locked()
        return event

    def cancel(self, event: ScheduledEvent) -> bool:
        with self._condition:
            cancelled = event.cancel()
            if cancelled:
                # Wake the pump so it prunes the event promptly; accounting
                # happens inside _prune_cancelled().
                self._condition.notify_all()
        return cancelled

    def cancel_all(self) -> int:
        with self._condition:
            count = 0
            for event in self._heap:
                if event.cancel():
                    count += 1
            self._condition.notify_all()
        return count

    def process_due(self) -> int:
        """Run every event due according to the clock.

        This is normally driven by the scheduler thread but is public so tests
        with an injected clock can advance time deterministically.

        Returns:
            The number of events dispatched.
        """
        with self._condition:
            due = self._pop_due_events()
        for event in due:
            self._dispatch(event)
        return len(due)

    def close(self, timeout: float | None = None) -> int:
        """Stop the scheduler thread and worker pool.

        Returns:
            Number of events left unexecuted according to ``close_policy``.
        """
        with self._condition:
            if self._closed:
                return len(self._heap)
            unexecuted, to_flush = self._apply_close_policy_locked()
            self._closed = True
            self._condition.notify_all()
        # Dispatch outside the lock so a callback or a full worker queue can
        # never deadlock against the scheduler condition.
        for event in to_flush:
            self._dispatch(event)
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        self._workers.shutdown(timeout)
        return unexecuted

    # -- internals ----------------------------------------------------------

    def _start_locked(self) -> None:
        if self._thread is not None:
            return
        self._workers.start()
        self._thread = threading.Thread(
            target=self._run, name="osc-scheduler", daemon=True
        )
        self._thread.start()

    def _apply_close_policy_locked(
        self,
    ) -> tuple[int, list[ScheduledEvent]]:
        """Return (unexecuted count, events to dispatch outside the lock)."""
        if self._close_policy is ClosePolicy.CANCEL_PENDING:
            unexecuted = 0
            for event in self._heap:
                if event.cancel():
                    unexecuted += 1
            # Single accounting point for the cancelled events.
            self._prune_cancelled()
            return unexecuted, []
        if self._close_policy is ClosePolicy.FLUSH:
            # Everything pending is "due" immediately when flushing.
            to_flush = list(self._heap)
            self._heap = []
            return 0, to_flush
        # WAIT_PENDING: leave the heap alone; the thread drains it.
        return 0, []

    def _dispatch(self, event: ScheduledEvent) -> None:
        if not event.pending:
            self._record_outcome(event)
            return

        def job() -> None:
            try:
                event.callback()
                event.state = EventState.FIRED
            except Exception as exc:  # callbacks must not kill the pool
                event.state = EventState.FIRED
                event.error = exc
                self._error_count += 1
                _logger.exception("scheduled OSC callback raised an exception")
            finally:
                self._record_outcome(event)

        self._workers.submit(job)

    def _run(self) -> None:
        while True:
            with self._condition:
                self._wakeup = False
                self._prune_cancelled()
                if self._closed and not self._heap:
                    return
                if self._closed and self._close_policy is ClosePolicy.CANCEL_PENDING:
                    return
                if not self._heap:
                    if self._closed:
                        return
                    self._condition.wait(timeout=0.1)
                    continue
                delay = self._heap[0].time - self._clock()
                if delay > 0:
                    self._condition.wait(timeout=min(delay, 0.1))
                    continue
                due = self._pop_due_events()
            for event in due:
                self._dispatch(event)


class AsyncIOEventScheduler(EventScheduler):
    """Bounded scheduler for the asyncio OSC servers.

    A single loop task wakes up at the next timetag and awaits the callbacks on
    the same event loop, so no threads are created and receiving datagrams is
    never blocked.  Synchronous callbacks are invoked directly; coroutine
    callbacks are awaited.
    """

    def __init__(
        self,
        capacity: int | None = 10000,
        overflow_policy: OverflowPolicy = OverflowPolicy.REJECT,
        close_policy: ClosePolicy = ClosePolicy.CANCEL_PENDING,
        clock: Callable[[], float] | None = None,
        listener: Listener | None = None,
        auto_start: bool = True,
    ) -> None:
        super().__init__(capacity, overflow_policy, close_policy, clock, listener)
        self._wakeup: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._auto_start = auto_start

    def start(self) -> None:
        """Start the pump task explicitly (must run inside an event loop)."""
        self._ensure_task()

    def schedule(self, when: float, callback: Callback) -> ScheduledEvent | None:
        event = self._enqueue_locked(when, callback)
        if event.state is not EventState.PENDING:
            return None
        if self._auto_start:
            self._ensure_task()
        if self._wakeup is not None:
            self._wakeup.set()
        return event

    def cancel(self, event: ScheduledEvent) -> bool:
        cancelled = event.cancel()
        if cancelled and self._wakeup is not None:
            # The pump prunes the event and accounts for it on its next pass.
            self._wakeup.set()
        return cancelled

    def cancel_all(self) -> int:
        count = 0
        for event in self._heap:
            if event.cancel():
                count += 1
        if count and self._wakeup is not None:
            self._wakeup.set()
        return count

    async def process_due(self) -> int:
        """Await every event due according to the clock.

        Driven automatically by the scheduler task; public for deterministic
        tests with an injected clock.

        Returns:
            The number of events dispatched.
        """
        due = self._pop_due_events()
        for event in due:
            await self._dispatch(event)
        return len(due)

    async def aclose(self, timeout: float | None = None) -> int:
        """Cancel the scheduler task and apply the close policy."""
        if self._closed:
            return len(self._heap)
        unexecuted = await self._apply_close_policy()
        self._closed = True
        if self._wakeup is not None:
            self._wakeup.set()
        task = self._task
        if task is not None and task is not asyncio.current_task():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                task.cancel()
        return unexecuted

    def close(self, timeout: float | None = None) -> int:
        """Synchronous close helper; only valid when no loop task is running."""
        if self._task is not None and not self._task.done():
            raise SchedulerError(
                "use aclose() to close an AsyncIOEventScheduler running on a loop"
            )
        unexecuted = 0
        if self._close_policy is ClosePolicy.CANCEL_PENDING:
            for event in self._heap:
                if event.cancel():
                    unexecuted += 1
            self._prune_cancelled()
        elif self._close_policy is ClosePolicy.FLUSH:
            due = self._heap
            self._heap = []
            for event in due:
                event.callback()
                event.state = EventState.FIRED
                self._record_outcome(event)
        else:
            raise SchedulerError(
                "WAIT_PENDING cannot be served by the synchronous close(); use aclose()"
            )
        self._closed = True
        return unexecuted

    # -- internals ----------------------------------------------------------

    def _ensure_task(self) -> None:
        if self._task is None or self._task.done():
            self._loop = asyncio.get_running_loop()
            self._wakeup = asyncio.Event()
            self._task = self._loop.create_task(self._run())

    async def _apply_close_policy(self) -> int:
        if self._close_policy is ClosePolicy.CANCEL_PENDING:
            unexecuted = 0
            for event in self._heap:
                if event.cancel():
                    unexecuted += 1
            self._prune_cancelled()
            return unexecuted
        if self._close_policy is ClosePolicy.FLUSH:
            due = self._heap
            self._heap = []
            for event in due:
                await self._dispatch(event)
            return 0
        return 0  # WAIT_PENDING: the pump task drains the heap

    async def _dispatch(self, event: ScheduledEvent) -> None:
        if not event.pending:
            self._record_outcome(event)
            return
        try:
            result = event.callback()
            if asyncio.iscoroutine(result):
                await result
            event.state = EventState.FIRED
        except Exception as exc:
            event.state = EventState.FIRED
            event.error = exc
            self._error_count += 1
            _logger.exception("scheduled OSC callback raised an exception")
        finally:
            self._record_outcome(event)

    async def _run(self) -> None:
        try:
            while True:
                self._prune_cancelled()
                if self._closed and not self._heap:
                    return
                if self._closed and self._close_policy is not ClosePolicy.WAIT_PENDING:
                    return
                if not self._heap:
                    if self._closed:
                        return
                    assert self._wakeup is not None
                    try:
                        await asyncio.wait_for(self._wakeup.wait(), timeout=0.1)
                    except asyncio.TimeoutError:
                        pass
                    self._wakeup.clear()
                    continue
                delay = self._heap[0].time - self._clock()
                if delay > 0:
                    assert self._wakeup is not None
                    try:
                        await asyncio.wait_for(
                            self._wakeup.wait(), timeout=min(delay, 0.1)
                        )
                    except asyncio.TimeoutError:
                        pass
                    self._wakeup.clear()
                    continue
                await self.process_due()
        except asyncio.CancelledError:
            if not self._closed:
                raise
