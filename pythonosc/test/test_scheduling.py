"""Tests for the bounded OSC event schedulers.

Both scheduler implementations are driven through the same battery of
scenarios using an injected, manually advanced clock, so timing is fully
deterministic and no real sleeping is involved.
"""

import asyncio
import threading
import unittest

from pythonosc.scheduling import (
    AsyncIOEventScheduler,
    ClosePolicy,
    EventState,
    OverflowPolicy,
    QueueFullError,
    ScheduledEvent,
    SchedulerClosedError,
    ThreadEventScheduler,
)


class FakeClock:
    """Controllable time source: time only moves when advance() is called."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class ThreadSchedulerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        # Manual mode: no auto-running thread, everything is pumped via
        # process_due() so the injected clock stays fully deterministic.
        self.scheduler = ThreadEventScheduler(
            capacity=10, clock=self.clock, max_workers=1, auto_start=False
        )

    def tearDown(self) -> None:
        self.scheduler.close(timeout=1)

    def test_runs_event_when_due_and_not_before(self):
        calls = []
        handle = self.scheduler.schedule(1005.0, lambda: calls.append("a"))
        self.assertIsInstance(handle, ScheduledEvent)
        self.scheduler.process_due()
        self.assertEqual([], calls)
        self.assertEqual(1, self.scheduler.stats().pending)

        self.clock.advance(4.9)
        self.scheduler.process_due()
        self.assertEqual([], calls)

        self.clock.advance(0.2)
        self.scheduler.process_due()
        self.assertEqual(["a"], calls)
        self.assertEqual(EventState.FIRED, handle.state)
        self.assertEqual(1, self.scheduler.stats().fired)
        self.assertEqual(0, self.scheduler.stats().pending)

    def test_expired_event_runs_immediately(self):
        calls = []
        self.scheduler.schedule(900.0, lambda: calls.append("late"))
        self.scheduler.process_due()
        self.assertEqual(["late"], calls)

    def test_ordering_same_timetag_preserves_insertion_order(self):
        calls = []
        for name in ("a", "b", "c", "d"):
            self.scheduler.schedule(1010.0, lambda n=name: calls.append(n))
        self.clock.advance(20)
        self.scheduler.process_due()
        self.assertEqual(["a", "b", "c", "d"], calls)

    def test_ordering_mixed_timetags(self):
        calls = []
        self.scheduler.schedule(1010.0, lambda: calls.append("late"))
        self.scheduler.schedule(1002.0, lambda: calls.append("early"))
        self.scheduler.schedule(1002.0, lambda: calls.append("early2"))
        self.clock.advance(20)
        self.scheduler.process_due()
        self.assertEqual(["early", "early2", "late"], calls)

    def test_cancel_single_event(self):
        calls = []
        handle = self.scheduler.schedule(1005.0, lambda: calls.append("x"))
        self.assertTrue(handle.cancel())
        self.assertFalse(handle.cancel())
        self.clock.advance(10)
        self.scheduler.process_due()
        self.assertEqual([], calls)
        stats = self.scheduler.stats()
        self.assertEqual(1, stats.cancelled)
        self.assertEqual(0, stats.pending)
        self.assertEqual(0, stats.fired)

    def test_cancel_all(self):
        calls = []
        for i in range(5):
            self.scheduler.schedule(1005.0 + i, lambda i=i: calls.append(i))
        cancelled = self.scheduler.cancel_all()
        self.assertEqual(5, cancelled)
        self.clock.advance(20)
        self.scheduler.process_due()
        self.assertEqual([], calls)
        self.assertEqual(0, self.scheduler.stats().pending)

    def test_overflow_reject(self):
        rejected = []
        scheduler = ThreadEventScheduler(
            capacity=2,
            overflow_policy=OverflowPolicy.REJECT,
            clock=self.clock,
            listener=lambda event: (
                rejected.append(event) if event.state is EventState.REJECTED else None
            ),
        )
        self.addCleanup(scheduler.close)
        self.assertIsNotNone(scheduler.schedule(1010, lambda: None))
        self.assertIsNotNone(scheduler.schedule(1011, lambda: None))
        self.assertIsNone(scheduler.schedule(1012, lambda: None))
        self.assertEqual(1, scheduler.stats().rejected)
        self.assertEqual(1, len(rejected))
        self.assertEqual(2, scheduler.stats().pending)

    def test_overflow_drop_newest_is_silent(self):
        events = []
        scheduler = ThreadEventScheduler(
            capacity=1,
            overflow_policy=OverflowPolicy.DROP_NEWEST,
            clock=self.clock,
            listener=events.append,
        )
        self.addCleanup(scheduler.close)
        scheduler.schedule(1010, lambda: None)
        self.assertIsNone(scheduler.schedule(1011, lambda: None))
        self.assertEqual(1, scheduler.stats().rejected)
        self.assertEqual([], events)

    def test_overflow_raise(self):
        scheduler = ThreadEventScheduler(
            capacity=1, overflow_policy=OverflowPolicy.RAISE, clock=self.clock
        )
        self.addCleanup(scheduler.close)
        scheduler.schedule(1010, lambda: None)
        with self.assertRaises(QueueFullError):
            scheduler.schedule(1011, lambda: None)

    def test_overflow_drop_oldest(self):
        dropped = []
        scheduler = ThreadEventScheduler(
            capacity=2,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
            clock=self.clock,
            listener=lambda event: (
                dropped.append(event) if event.state is EventState.DROPPED else None
            ),
        )
        self.addCleanup(scheduler.close)
        oldest = scheduler.schedule(1010, lambda: None)
        scheduler.schedule(1011, lambda: None)
        newest = scheduler.schedule(1012, lambda: None)
        self.assertEqual(1, scheduler.stats().dropped)
        self.assertEqual(EventState.DROPPED, oldest.state)
        self.assertEqual(EventState.PENDING, newest.state)
        self.assertEqual(1, len(dropped))

    def test_close_cancels_pending(self):
        for i in range(3):
            self.scheduler.schedule(1010 + i, lambda: None)
        unexecuted = self.scheduler.close(timeout=1)
        self.assertEqual(3, unexecuted)
        self.assertTrue(self.scheduler.closed)
        self.assertEqual(3, self.scheduler.stats().cancelled)
        with self.assertRaises(SchedulerClosedError):
            self.scheduler.schedule(1020, lambda: None)

    def test_close_flush_runs_everything_immediately(self):
        calls = []
        scheduler = ThreadEventScheduler(
            capacity=10,
            clock=self.clock,
            close_policy=ClosePolicy.FLUSH,
        )
        scheduler.schedule(1030, lambda: calls.append("a"))
        scheduler.schedule(1040, lambda: calls.append("b"))
        unexecuted = scheduler.close(timeout=1)
        self.assertEqual(0, unexecuted)
        self.assertEqual(["a", "b"], calls)

    def test_close_wait_pending_drains_heap(self):
        import time as _time

        calls = []
        # Real clock and an auto-starting pump thread for WAIT_PENDING.
        scheduler = ThreadEventScheduler(
            capacity=10, close_policy=ClosePolicy.WAIT_PENDING
        )
        base = _time.time() + 0.2
        for i in range(5):
            scheduler.schedule(base + i * 0.01, lambda i=i: calls.append(i))
        unexecuted = scheduler.close(timeout=5)
        self.assertEqual(0, unexecuted)
        self.assertEqual([0, 1, 2, 3, 4], calls)

    def test_callback_exception_is_caught_and_counted(self):
        def boom() -> None:
            raise RuntimeError("nope")

        self.scheduler.schedule(1001, boom)
        self.clock.advance(2)
        self.scheduler.process_due()
        stats = self.scheduler.stats()
        self.assertEqual(1, stats.fired)
        self.assertEqual(1, stats.errors)
        # Scheduler keeps working after a failing callback.
        ok = []
        self.scheduler.schedule(1003, lambda: ok.append("ok"))
        self.clock.advance(3)
        self.scheduler.process_due()
        self.assertEqual(["ok"], ok)

    def test_listener_sees_all_transitions(self):
        seen = []
        scheduler = ThreadEventScheduler(
            clock=self.clock,
            listener=lambda event: seen.append(event.state),
            auto_start=False,
        )
        self.addCleanup(scheduler.close)
        scheduler.schedule(1005, lambda: None)
        self.clock.advance(10)
        scheduler.process_due()
        handle2 = scheduler.schedule(1020, lambda: None)
        handle2.cancel()
        scheduler.process_due()
        self.assertIn(EventState.FIRED, seen)
        self.assertIn(EventState.CANCELLED, seen)

    def test_background_thread_fires_events_on_real_clock(self):
        calls = []
        scheduler = ThreadEventScheduler(capacity=5, max_workers=1)
        self.addCleanup(scheduler.close)
        event = threading.Event()
        scheduler.schedule(0.05, lambda: (calls.append(1), event.set()))
        self.assertTrue(event.wait(timeout=2))
        self.assertEqual([1], calls)

    def test_invalid_capacity(self):
        with self.assertRaises(ValueError):
            ThreadEventScheduler(capacity=0)

    def test_pending_events_snapshot_order(self):
        h1 = self.scheduler.schedule(1010, lambda: None)
        h2 = self.scheduler.schedule(1005, lambda: None)
        pending = self.scheduler.pending_events()
        self.assertEqual([h2, h1], pending)


class AsyncIOSchedulerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        # Manual mode by default: process_due() drives dispatch against the
        # injected clock.  Tests that want the background pump task build their
        # own scheduler with auto_start=True.
        self.scheduler = AsyncIOEventScheduler(
            capacity=10, clock=self.clock, auto_start=False
        )

    async def asyncTearDown(self) -> None:
        if not self.scheduler.closed:
            await self.scheduler.aclose(timeout=1)

    async def test_runs_event_when_due_and_not_before(self):
        calls = []
        handle = self.scheduler.schedule(1005.0, lambda: calls.append("a"))
        self.clock.advance(4)
        self.assertEqual(0, await self.scheduler.process_due())
        self.assertEqual([], calls)
        self.clock.advance(2)
        self.assertEqual(1, await self.scheduler.process_due())
        self.assertEqual(["a"], calls)
        self.assertEqual(EventState.FIRED, handle.state)

    async def test_background_pump_fires_on_advanced_clock(self):
        calls = []
        scheduler = AsyncIOEventScheduler(clock=self.clock)
        scheduler.schedule(1005.0, lambda: calls.append("a"))
        # Not due yet: pump task must not fire it.
        await asyncio.sleep(0.05)
        self.assertEqual([], calls)
        self.clock.advance(6)
        # Pump polls the clock roughly every 100ms.
        await asyncio.sleep(0.15)
        self.assertEqual(["a"], calls)
        await scheduler.aclose()

    async def test_manual_process_due_with_fake_clock(self):
        calls = []
        self.scheduler.schedule(1005.0, lambda: calls.append("a"))
        self.clock.advance(6)
        count = await self.scheduler.process_due()
        self.assertEqual(1, count)
        self.assertEqual(["a"], calls)

    async def test_coroutine_callback_is_awaited(self):
        calls = []

        async def cb() -> None:
            await asyncio.sleep(0)
            calls.append("async")

        self.scheduler.schedule(1002.0, cb)
        self.clock.advance(5)
        await self.scheduler.process_due()
        self.assertEqual(["async"], calls)

    async def test_same_timetag_order_preserved(self):
        calls = []
        for name in ("a", "b", "c"):
            self.scheduler.schedule(1010.0, lambda n=name: calls.append(n))
        self.clock.advance(20)
        await self.scheduler.process_due()
        self.assertEqual(["a", "b", "c"], calls)

    async def test_cancel_and_cancel_all(self):
        calls = []
        handle = self.scheduler.schedule(1010, lambda: calls.append("x"))
        self.assertTrue(handle.cancel())
        for i in range(3):
            self.scheduler.schedule(1011 + i, lambda i=i: calls.append(i))
        self.assertEqual(3, self.scheduler.cancel_all())
        self.clock.advance(20)
        await self.scheduler.process_due()
        self.assertEqual([], calls)
        self.assertEqual(4, self.scheduler.stats().cancelled)

    async def test_overflow_policies(self):
        rejected = []
        scheduler = AsyncIOEventScheduler(
            capacity=1,
            overflow_policy=OverflowPolicy.REJECT,
            clock=self.clock,
            listener=lambda event: (
                rejected.append(event) if event.state is EventState.REJECTED else None
            ),
        )
        self.assertIsNotNone(scheduler.schedule(1010, lambda: None))
        self.assertIsNone(scheduler.schedule(1011, lambda: None))
        self.assertEqual(1, scheduler.stats().rejected)
        self.assertEqual(1, len(rejected))
        await scheduler.aclose()

    async def test_close_cancel_pending(self):
        for i in range(3):
            self.scheduler.schedule(1010 + i, lambda: None)
        unexecuted = await self.scheduler.aclose(timeout=1)
        self.assertEqual(3, unexecuted)
        self.assertEqual(3, self.scheduler.stats().cancelled)
        with self.assertRaises(SchedulerClosedError):
            self.scheduler.schedule(1020, lambda: None)

    async def test_close_flush_awaits_coroutines(self):
        calls = []

        async def cb(name: str) -> None:
            await asyncio.sleep(0)
            calls.append(name)

        scheduler = AsyncIOEventScheduler(
            clock=self.clock, close_policy=ClosePolicy.FLUSH
        )
        scheduler.schedule(1030, lambda: cb("a"))
        scheduler.schedule(1040, lambda: cb("b"))
        unexecuted = await scheduler.aclose(timeout=1)
        self.assertEqual(0, unexecuted)
        self.assertEqual(["a", "b"], calls)

    async def test_close_wait_pending_drains_heap(self):
        calls = []
        # Manual scheduler so we can start the pump after deciding to wait.
        scheduler = AsyncIOEventScheduler(
            clock=self.clock, close_policy=ClosePolicy.WAIT_PENDING
        )
        scheduler.schedule(1002, lambda: calls.append("a"))
        scheduler.schedule(1003, lambda: calls.append("b"))
        self.clock.advance(5)
        # Closing with WAIT_PENDING lets the pump run all pending events first.
        unexecuted = await scheduler.aclose(timeout=2)
        self.assertEqual(0, unexecuted)
        self.assertEqual(["a", "b"], calls)

    async def test_callback_exception_isolated(self):
        def boom() -> None:
            raise RuntimeError("nope")

        self.scheduler.schedule(1001, boom)
        ok = []
        self.scheduler.schedule(1002, lambda: ok.append("ok"))
        self.clock.advance(5)
        await self.scheduler.process_due()
        self.assertEqual(["ok"], ok)
        self.assertEqual(1, self.scheduler.stats().errors)
        self.assertEqual(2, self.scheduler.stats().fired)

    async def test_schedule_on_closed_scheduler_raises(self):
        await self.scheduler.aclose()
        with self.assertRaises(SchedulerClosedError):
            self.scheduler.schedule(1020, lambda: None)


class DispatcherIntegrationTest(unittest.IsolatedAsyncioTestCase):
    """End-to-end behaviour through the Dispatcher with a fake clock."""

    @staticmethod
    def _bundle(timetag: float, address: str) -> bytes:
        from pythonosc.osc_bundle_builder import OscBundleBuilder
        from pythonosc.osc_message_builder import OscMessageBuilder

        msg = OscMessageBuilder(address).build()
        builder = OscBundleBuilder(timetag)
        builder.add_content(msg)
        return builder.build().dgram

    @staticmethod
    def _nested_bundle(outer_tag: float, inner_tag: float, *addresses: str) -> bytes:
        from pythonosc.osc_bundle_builder import OscBundleBuilder
        from pythonosc.osc_message_builder import OscMessageBuilder

        inner = OscBundleBuilder(inner_tag)
        for address in addresses[1:]:
            inner.add_content(OscMessageBuilder(address).build())
        outer = OscBundleBuilder(outer_tag)
        outer.add_content(OscMessageBuilder(addresses[0]).build())
        outer.add_content(inner.build())
        return outer.build().dgram

    def test_nested_bundle_timing_and_same_time_order(self):
        """Requirement 1: nested timetags and same-time order are preserved."""
        from pythonosc.osc_packet import OscPacket

        clock = FakeClock()
        outer_tag = clock() + 10  # 1010
        inner_tag = clock() + 5  # 1005
        dgram = self._nested_bundle(outer_tag, inner_tag, "/a", "/b", "/c")

        packet = OscPacket(dgram)
        raw_times = {m.message.address: m.raw_time for m in packet.messages}
        # Each nested bundle keeps its own timetag in raw_time: the outer
        # message /a fires at 1010, the inner-bundle messages /b and /c at 1005.
        self.assertEqual({"/a": outer_tag, "/b": inner_tag, "/c": inner_tag}, raw_times)

        # The inner-bundle messages share a timetag and keep their original
        # (on-the-wire) order relative to each other.
        inner = [m for m in packet.messages if m.raw_time == inner_tag]
        self.assertEqual(["/b", "/c"], [m.message.address for m in inner])

        # Events fire in timetag order, with same-timetag messages in wire
        # order, through the dispatcher/scheduler.
        fired = []
        scheduler = ThreadEventScheduler(clock=clock, max_workers=1, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        dispatcher = Dispatcher(scheduler=scheduler)
        for address in ("/a", "/b", "/c"):
            dispatcher.map(address, lambda addr, a=address: fired.append(a))
        dispatcher.call_handlers_for_packet(dgram, ("127.0.0.1", 1))
        self.assertEqual([], fired)
        clock.advance(11)
        scheduler.process_due()
        self.assertEqual(["/b", "/c", "/a"], fired)
        scheduler.close()

    def test_past_and_immediate_events_run_at_once(self):
        """Requirement 1: stale and IMMEDIATELY events are not deferred."""
        import time as _time

        # Use the real wall clock so the parser's own "now" agrees with the
        # scheduler's clock when deciding that a timetag is in the past.
        scheduler = ThreadEventScheduler(max_workers=1, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        fired = []
        dispatcher = Dispatcher(scheduler=scheduler)
        dispatcher.map("/go", lambda address: fired.append(address))

        # A timetag already in the past.
        dispatcher.call_handlers_for_packet(
            self._bundle(_time.time() - 50, "/go"), ("127.0.0.1", 1)
        )
        # An IMMEDIATELY bundle (timetag 0).
        dispatcher.call_handlers_for_packet(self._bundle(0, "/go"), ("127.0.0.1", 1))
        # Both must fire immediately, nothing may be queued for later.
        self.assertEqual(["/go", "/go"], fired)
        self.assertEqual(0, scheduler.stats().pending)
        scheduler.close()

    def test_sync_dispatcher_defers_and_cancels(self):
        calls = []
        clock = FakeClock()
        scheduler = ThreadEventScheduler(clock=clock, max_workers=1, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        dispatcher = Dispatcher(scheduler=scheduler)
        dispatcher.map("/go", lambda address: calls.append(address))

        dgram = self._bundle(clock() + 10, "/go")
        # Returns immediately: no sleeping on the receive path.
        result = dispatcher.call_handlers_for_packet(dgram, ("127.0.0.1", 1))
        self.assertEqual([], result)
        self.assertEqual([], calls)
        self.assertEqual(1, scheduler.stats().pending)

        # Cancellation before the timetag prevents the callback.
        dispatcher.cancel_scheduled()
        clock.advance(20)
        scheduler.process_due()
        self.assertEqual([], calls)
        scheduler.close()

    def test_sync_dispatcher_fires_at_timetag_and_immediate_bundle(self):
        calls = []
        clock = FakeClock()
        scheduler = ThreadEventScheduler(clock=clock, max_workers=1, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        dispatcher = Dispatcher(scheduler=scheduler)
        dispatcher.map("/go", lambda address: calls.append(address))

        future = self._bundle(clock() + 10, "/go")
        dispatcher.call_handlers_for_packet(future, ("127.0.0.1", 1))
        # IMMEDIATELY bundles (timetag 0) must fire at once.
        immediate = self._bundle(0, "/go")
        dispatcher.call_handlers_for_packet(immediate, ("127.0.0.1", 1))
        self.assertEqual(["/go"], calls)

        clock.advance(11)
        scheduler.process_due()
        self.assertEqual(["/go", "/go"], calls)
        scheduler.close()

    async def test_async_dispatcher_with_async_scheduler(self):
        calls = []
        clock = FakeClock()
        scheduler = AsyncIOEventScheduler(clock=clock, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        dispatcher = Dispatcher(scheduler=scheduler)
        dispatcher.map("/go", lambda address: calls.append(address))

        dgram = self._bundle(clock() + 10, "/go")
        await dispatcher.async_call_handlers_for_packet(dgram, ("127.0.0.1", 1))
        self.assertEqual([], calls)
        self.assertEqual(1, scheduler.stats().pending)

        clock.advance(11)
        await scheduler.process_due()
        self.assertEqual(["/go"], calls)
        await scheduler.aclose()

    async def test_slow_callback_does_not_block_reception(self):
        # The receive path must return even while a scheduled callback is slow.
        import time as _time

        clock = FakeClock()
        scheduler = AsyncIOEventScheduler(clock=clock, auto_start=False)
        from pythonosc.dispatcher import Dispatcher

        dispatcher = Dispatcher(scheduler=scheduler)

        async def slow(address: str) -> None:
            await asyncio.sleep(0.05)

        dispatcher.map("/slow", slow)
        dgram = self._bundle(clock() + 5, "/slow")
        start = _time.monotonic()
        await dispatcher.async_call_handlers_for_packet(dgram, ("127.0.0.1", 1))
        elapsed = _time.monotonic() - start
        self.assertLess(elapsed, 0.05)
        clock.advance(10)
        await scheduler.process_due()
        self.assertEqual(1, scheduler.stats().fired)
        await scheduler.aclose()

    def test_strict_and_immediate_modes_unchanged_without_scheduler(self):
        from pythonosc.dispatcher import Dispatcher

        # Default dispatcher: no scheduler.
        self.assertIsNone(Dispatcher().scheduler)
        self.assertIsNone(Dispatcher(strict_timing=False).scheduler)
        self.assertEqual(0, Dispatcher().cancel_scheduled())


class SyncUDPServerIntegrationTest(unittest.TestCase):
    """End-to-end: a real loopback UDP server using the bounded scheduler."""

    def test_future_bundle_is_received_fast_and_fires_later(self):
        import time as _time

        from pythonosc.dispatcher import Dispatcher
        from pythonosc.osc_bundle_builder import OscBundleBuilder
        from pythonosc.osc_message_builder import OscMessageBuilder
        from pythonosc.osc_server import ThreadingOSCUDPServer
        from pythonosc.udp_client import SimpleUDPClient

        fired = threading.Event()
        scheduler = ThreadEventScheduler(capacity=100, max_workers=2)
        dispatcher = Dispatcher(scheduler=scheduler)
        dispatcher.map("/cue", lambda address: fired.set())

        server = ThreadingOSCUDPServer(("127.0.0.1", 0), dispatcher)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        self.addCleanup(scheduler.close)

        client = SimpleUDPClient("127.0.0.1", port)
        msg = OscMessageBuilder("/cue").build()
        builder = OscBundleBuilder(_time.time() + 0.4)
        builder.add_content(msg)

        start = _time.monotonic()
        client.send(builder.build())
        # Give the server thread a moment to process the datagram.
        deadline = start + 1
        while scheduler.stats().pending == 0 and _time.monotonic() < deadline:
            _time.sleep(0.01)
        enqueue_elapsed = _time.monotonic() - start
        self.assertEqual(1, scheduler.stats().pending)
        # Reception did not block for the ~0.4s timetag.
        self.assertLess(enqueue_elapsed, 0.25)
        # The callback fires only once the timetag is reached.
        self.assertTrue(fired.wait(timeout=2))


class AsyncIOUDPServerIntegrationTest(unittest.IsolatedAsyncioTestCase):
    """End-to-end: a real asyncio UDP server using the asyncio scheduler."""

    async def test_future_bundle_fires_on_event_loop(self):
        import time as _time

        from pythonosc.dispatcher import Dispatcher
        from pythonosc.osc_bundle_builder import OscBundleBuilder
        from pythonosc.osc_message_builder import OscMessageBuilder
        from pythonosc.osc_server import AsyncIOOSCUDPServer
        from pythonosc.udp_client import SimpleUDPClient

        fired = asyncio.Event()
        scheduler = AsyncIOEventScheduler(capacity=100)
        dispatcher = Dispatcher(scheduler=scheduler)

        def on_cue(address: str) -> None:
            fired.set()

        dispatcher.map("/cue", on_cue)

        loop = asyncio.get_running_loop()
        server = AsyncIOOSCUDPServer(("127.0.0.1", 0), dispatcher, loop)
        transport, _ = await server.create_serve_endpoint()

        async def cleanup() -> None:
            transport.close()
            await scheduler.aclose()

        self.addCleanup(lambda: loop.run_until_complete(cleanup()))

        port = transport.get_extra_info("socket").getsockname()[1]
        client = SimpleUDPClient("127.0.0.1", port)
        msg = OscMessageBuilder("/cue").build()
        builder = OscBundleBuilder(_time.time() + 0.3)
        builder.add_content(msg)
        client.send(builder.build())

        # Wait for the datagram to be enqueued, then confirm deferred firing.
        deadline = loop.time() + 1
        while scheduler.stats().pending == 0 and loop.time() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(1, scheduler.stats().pending)
        await asyncio.wait_for(fired.wait(), timeout=2)


if __name__ == "__main__":
    unittest.main()
