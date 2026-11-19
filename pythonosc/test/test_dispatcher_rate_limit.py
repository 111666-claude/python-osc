import asyncio
import unittest

from pythonosc import osc_bundle_builder, osc_message_builder
from pythonosc.dispatcher import Dispatcher
from pythonosc.rate_limit import OverflowPolicy, RateLimitError


class VirtualClock:
    """Manually controlled clock plus matching sleep hooks."""

    def __init__(self, start: float = 1000.0):
        self.time = float(start)

    def __call__(self):
        return self.time

    def advance(self, seconds):
        self.time += seconds

    def rollback(self, seconds):
        self.time -= seconds

    def sleep(self, seconds):
        self.time += seconds

    async def async_sleep(self, seconds):
        self.time += seconds


def msg(address, *args):
    builder = osc_message_builder.OscMessageBuilder(address)
    for arg in args:
        builder.add_arg(arg)
    return builder.build().dgram


def bundle(timetag, *messages):
    builder = osc_bundle_builder.OscBundleBuilder(timetag)
    for message in messages:
        builder.add_content(message)
    return builder.build().dgram


class TestDispatcherRateLimit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.clock = VirtualClock()
        # auto_schedule=False: every delivery is driven through the injected
        # virtual clock and explicit drains, so tests stay deterministic.
        self.dispatcher = Dispatcher(
            clock=self.clock,
            sleep=self.clock.sleep,
            async_sleep=self.clock.async_sleep,
            auto_schedule=False,
        )

    def test_no_rate_limit_keeps_legacy_behavior(self):
        received = []
        self.dispatcher.map("/free", lambda address, *a: received.append(a))
        for value in range(20):
            self.dispatcher.call_handlers_for_packet(
                msg("/free", value), ("127.0.0.1", 9000)
            )
        self.assertEqual(list(range(20)), [args[0] for args in received])

    def test_dual_address_burst_is_limited_independently(self):
        slow = []
        fast = []
        # 1/s sustained, burst of 4 messages per route so the bounded queue
        # (capacity = burst) can hold every backlog in this short test.
        self.dispatcher.map(
            "/slow", lambda address, v: slow.append(v), rate=1, burst=4
        )
        self.dispatcher.map(
            "/fast", lambda address, v: fast.append(v), rate=1, burst=4
        )
        for value in range(10):
            self.dispatcher.call_handlers_for_packet(
                msg("/slow", value), ("127.0.0.1", 9000)
            )
            self.dispatcher.call_handlers_for_packet(
                msg("/fast", 100 + value), ("127.0.0.1", 9000)
            )

        # Both routes delivered their burst of 4; the queue (capacity 4) then
        # holds the last four backlogged messages while two older ones are
        # evicted per route.
        self.assertEqual([0, 1, 2, 3], slow)
        self.assertEqual([100, 101, 102, 103], fast)
        slow_stats = self.dispatcher.rate_limit_stats("/slow")
        fast_stats = self.dispatcher.rate_limit_stats("/fast")
        self.assertEqual(10, slow_stats.submitted)
        self.assertEqual(10, fast_stats.submitted)
        self.assertEqual(4, slow_stats.delivered)
        self.assertEqual(4, fast_stats.delivered)
        self.assertEqual(2, slow_stats.dropped_old)
        self.assertEqual(2, fast_stats.dropped_old)
        self.assertEqual(4, slow_stats.queued)

        # The fast address cannot starve the slow one: after 1s each route
        # releases exactly one more message.
        self.clock.advance(1)
        delivered = self.dispatcher.drain_pending()
        self.assertEqual(2, delivered)
        self.assertEqual([0, 1, 2, 3, 6], slow)
        self.assertEqual([100, 101, 102, 103, 106], fast)

        # Advance enough to drain everything still queued; only the latest
        # values survive DROP_OLDEST eviction.
        self.clock.advance(10)
        self.dispatcher.drain_pending()
        self.assertEqual([0, 1, 2, 3, 6, 7, 8, 9], slow)
        self.assertEqual(
            [100, 101, 102, 103, 106, 107, 108, 109], fast
        )
        final_slow = self.dispatcher.rate_limit_stats("/slow")
        final_fast = self.dispatcher.rate_limit_stats("/fast")
        self.assertEqual(2, final_slow.dropped)
        self.assertEqual(2, final_fast.dropped)

    def test_drop_oldest_discards_superseded_values(self):
        received = []
        self.dispatcher.map(
            "/knob",
            lambda address, v: received.append(v),
            rate=1,
            burst=1,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        for value in range(5):
            self.dispatcher.call_handlers_for_packet(
                msg("/knob", value), ("127.0.0.1", 9000)
            )
        # Only the first value was delivered; the queue holds the latest.
        self.assertEqual([0], received)
        stats = self.dispatcher.rate_limit_stats("/knob")
        self.assertEqual(3, stats.dropped_old)
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        self.assertEqual([0, 4], received)

    def test_reject_new_drops_incoming_values(self):
        received = []
        self.dispatcher.map(
            "/knob",
            lambda address, v: received.append(v),
            rate=1,
            burst=1,
            overflow_policy=OverflowPolicy.REJECT_NEW,
        )
        for value in range(5):
            self.dispatcher.call_handlers_for_packet(
                msg("/knob", value), ("127.0.0.1", 9000)
            )
        self.assertEqual([0], received)
        stats = self.dispatcher.rate_limit_stats("/knob")
        self.assertEqual(3, stats.rejected)
        self.assertEqual(0, stats.dropped_old)
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        # The one queued value (value 1) survives; 2-4 were rejected.
        self.assertEqual([0, 1], received)

    def test_multiple_handlers_share_one_route_limiter(self):
        calls_a = []
        calls_b = []
        # Burst 3 so all three messages fit; the queue preserves FIFO order
        # for the two that cannot be delivered within the initial burst...
        # here burst 2: first two deliver, third queues and releases at +1s.
        self.dispatcher.map(
            "/shared", lambda address, v: calls_a.append(v), rate=1, burst=2
        )
        self.dispatcher.map(
            "/shared", lambda address, v: calls_b.append(v)
        )
        for value in range(3):
            self.dispatcher.call_handlers_for_packet(
                msg("/shared", value), ("127.0.0.1", 9000)
            )
        # First two messages fan out to both handlers at once; the third is
        # held by the single shared route limiter.
        self.assertEqual([0, 1], calls_a)
        self.assertEqual([0, 1], calls_b)
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        self.assertEqual([0, 1, 2], calls_a)
        self.assertEqual([0, 1, 2], calls_b)

    def test_wildcard_routes_are_limited_independently(self):
        concrete = []
        wildcard = []
        self.dispatcher.map(
            "/knob/a", lambda address, v: concrete.append(v), rate=1, burst=1
        )
        self.dispatcher.map(
            "/knob/*", lambda address, v: wildcard.append(v), rate=1, burst=1
        )
        for value in range(3):
            self.dispatcher.call_handlers_for_packet(
                msg("/knob/a", value), ("127.0.0.1", 9000)
            )
        self.assertEqual([0], concrete)
        self.assertEqual([0], wildcard)
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        # Latest value wins on each independent route (DROP_OLDEST).
        self.assertEqual([0, 2], concrete)
        self.assertEqual([0, 2], wildcard)
        stats = self.dispatcher.rate_limit_stats()
        self.assertEqual({"/knob/a", "/knob/*"}, set(stats))

    def test_rejection_by_one_route_rolls_back_the_other(self):
        first = []
        second = []
        # Both routes burst 1 and queue one message. The concrete route would
        # keep the latest value; the wildcard route rejects anything past its
        # single queue slot.
        self.dispatcher.map(
            "/knob/a",
            lambda address, v: first.append(v),
            rate=1,
            burst=1,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        self.dispatcher.map(
            "/knob/*",
            lambda address, v: second.append(v),
            rate=1,
            burst=1,
            overflow_policy=OverflowPolicy.REJECT_NEW,
        )
        # Message 0: delivered on both routes.
        self.dispatcher.call_handlers_for_packet(
            msg("/knob/a", 0), ("127.0.0.1", 9000)
        )
        # Message 1: queued on both routes.
        self.dispatcher.call_handlers_for_packet(
            msg("/knob/a", 1), ("127.0.0.1", 9000)
        )
        # Message 2: the wildcard route rejects it; the concrete route's
        # eviction of value 1 must be rolled back, so value 1 survives there.
        self.dispatcher.call_handlers_for_packet(
            msg("/knob/a", 2), ("127.0.0.1", 9000)
        )
        wildcard_stats = self.dispatcher.rate_limit_stats("/knob/*")
        concrete_stats = self.dispatcher.rate_limit_stats("/knob/a")
        self.assertEqual(1, wildcard_stats.rejected)
        self.assertEqual(0, concrete_stats.dropped_old)
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        self.assertEqual([0, 1], first)
        self.assertEqual([0, 1], second)

    def test_clock_rollback_does_not_release_queued_messages(self):
        received = []
        self.dispatcher.map(
            "/knob", lambda address, v: received.append(v), rate=1, burst=1
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 1), ("127.0.0.1", 9000)
        )
        self.clock.advance(5)
        self.clock.rollback(4.5)
        self.assertEqual(0, self.dispatcher.drain_pending())
        self.assertEqual([0], received)
        # Time still has to pass from the new anchor before tokens accrue.
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        self.assertEqual([0, 1], received)

    def test_strict_timing_false_ignores_queued_timetag(self):
        lax = Dispatcher(
            clock=self.clock,
            sleep=self.clock.sleep,
            async_sleep=self.clock.async_sleep,
            auto_schedule=False,
            strict_timing=False,
        )
        received = []
        lax.map("/knob", lambda address, v: received.append(v), rate=1, burst=1)
        lax.call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        future = bundle(
            self.clock.time + 100,
            osc_message_builder.build_msg("/knob", 1),
        )
        lax.call_handlers_for_packet(future, ("127.0.0.1", 9000))
        # Only one second (the token refill) is needed: the far-future
        # timetag is ignored because strict timing is disabled.
        self.clock.advance(1)
        lax.drain_pending()
        self.assertEqual([0, 1], received)

    def test_timed_bundle_honors_timetag_and_rate_limit_order(self):
        received = []
        base = self.clock.time
        # Burst of 1 so only value 0 delivers inline; the queue (capacity 1)
        # is kept small on purpose, so each queued value is checked in turn.
        self.dispatcher.map(
            "/knob",
            lambda address, v: received.append((self.clock.time, v)),
            rate=1,
            burst=1,
        )
        # Immediate message 0 delivers, immediate message 1 queues.
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 1), ("127.0.0.1", 9000)
        )
        self.assertEqual([(base, 0)], received)
        # At +1s value 1 has both a token and a past timetag, so it releases.
        self.clock.advance(1)
        self.dispatcher.drain_pending()
        self.assertEqual([(base, 0), (base + 1, 1)], received)

        # Now queue a future-timetag message. Even once its token is available
        # it must wait for the timetag, preserving OSC timing.
        future_packet = bundle(
            base + 10,
            osc_message_builder.build_msg("/knob", 2),
        )
        self.dispatcher.call_handlers_for_packet(
            future_packet, ("127.0.0.1", 9000)
        )
        self.clock.advance(2)
        self.dispatcher.drain_pending()
        # Token is ready (1s elapsed) but the timetag is still 7s away.
        self.assertEqual([(base, 0), (base + 1, 1)], received)
        self.clock.advance(7)
        self.dispatcher.drain_pending()
        self.assertEqual(
            [(base, 0), (base + 1, 1), (base + 10, 2)], received
        )

    def test_timed_bundle_orders_contents_by_timetag(self):
        # One bundle carrying two future messages is parsed into timetag-sorted
        # TimedMessages, and the limiter's FIFO queue preserves that order.
        delivered_values = []
        base = self.clock.time
        self.dispatcher.map(
            "/knob",
            lambda address, v: delivered_values.append(v),
            rate=10,
            burst=3,
        )
        outer = osc_bundle_builder.OscBundleBuilder(
            osc_bundle_builder.IMMEDIATELY
        )
        later = osc_bundle_builder.OscBundleBuilder(base + 2)
        later.add_content(osc_message_builder.build_msg("/knob", 1))
        sooner = osc_bundle_builder.OscBundleBuilder(base + 1)
        sooner.add_content(osc_message_builder.build_msg("/knob", 0))
        # Insert the later-timetag content first; parsing must still sort it.
        outer.add_content(later.build())
        outer.add_content(sooner.build())
        self.dispatcher.call_handlers_for_packet(
            outer.build().dgram, ("127.0.0.1", 9000)
        )
        # Both are future-dated, so nothing delivers at submission time.
        self.assertEqual([], delivered_values)
        self.clock.advance(3)
        self.dispatcher.drain_pending()
        # Sorted by timetag regardless of bundle insertion order.
        self.assertEqual([0, 1], delivered_values)

    def test_shutdown_drops_queued_messages_by_default(self):
        received = []
        self.dispatcher.map(
            "/knob", lambda address, v: received.append(v), rate=1, burst=2
        )
        for value in range(4):
            self.dispatcher.call_handlers_for_packet(
                msg("/knob", value), ("127.0.0.1", 9000)
            )
        dropped = self.dispatcher.shutdown()
        self.assertEqual(2, dropped)
        self.assertEqual([0, 1], received)
        # Idempotent.
        self.assertEqual(0, self.dispatcher.shutdown())
        # Unlimited routes keep working after shutdown; limited ones reject.
        self.dispatcher.map("/other", lambda address, v: received.append(v))
        self.dispatcher.call_handlers_for_packet(
            msg("/other", 99), ("127.0.0.1", 9000)
        )
        self.assertIn(99, received)

    def test_shutdown_flush_delivers_everything_in_timetag_order(self):
        received = []
        base = self.clock.time

        def record(address, v):
            received.append((self.clock.time, v))

        self.dispatcher.map("/knob", record, rate=1, burst=4)
        packet = bundle(
            base + 5,
            osc_message_builder.build_msg("/knob", 7),
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 1), ("127.0.0.1", 9000)
        )
        self.dispatcher.call_handlers_for_packet(
            packet, ("127.0.0.1", 9000)
        )
        dropped = self.dispatcher.shutdown(flush=True)
        self.assertEqual(0, dropped)
        # Flush sleeps through the timetag; both queued values are delivered
        # in timetag order (value 1 is already due, value 7 waits until +5s).
        self.assertEqual([(base, 0), (base, 1), (base + 5, 7)], received)

    def test_invalid_rate_parameters_raise(self):
        with self.assertRaises(RateLimitError):
            self.dispatcher.map("/a", lambda address: None, rate=0)
        with self.assertRaises(RateLimitError):
            self.dispatcher.map("/b", lambda address: None, rate=2, burst=0)
        with self.assertRaises(RateLimitError):
            self.dispatcher.set_rate_limit("/c", -1)

    def test_conflicting_remap_raises(self):
        self.dispatcher.map("/a", lambda address: None, rate=2, burst=2)
        with self.assertRaises(RateLimitError):
            self.dispatcher.map("/a", lambda address: None, rate=4)
        # Re-mapping with the same parameters is fine.
        self.dispatcher.map("/a", lambda address: None, rate=2, burst=2)
        # set_rate_limit explicitly changes the configuration.
        self.dispatcher.set_rate_limit("/a", 5, burst=5)
        self.dispatcher.set_rate_limit("/a", None)
        stats = self.dispatcher.rate_limit_stats()
        self.assertNotIn("/a", stats)

    def test_unmap_retires_queued_messages(self):
        received = []
        handler = self.dispatcher.map(
            "/knob", lambda address, v: received.append(v), rate=1, burst=1
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        self.dispatcher.call_handlers_for_packet(
            msg("/knob", 1), ("127.0.0.1", 9000)
        )
        self.dispatcher.unmap("/knob", handler)
        self.clock.advance(5)
        self.dispatcher.drain_pending()
        self.assertEqual([0], received)

    async def test_async_dispatch_and_ashutdown(self):
        received = []

        async def handler(address, v):
            received.append(v)

        self.dispatcher.map("/knob", handler, rate=1, burst=1)
        await self.dispatcher.async_call_handlers_for_packet(
            msg("/knob", 0), ("127.0.0.1", 9000)
        )
        await self.dispatcher.async_call_handlers_for_packet(
            msg("/knob", 1), ("127.0.0.1", 9000)
        )
        self.assertEqual([0], received)
        self.clock.advance(1)
        await self.dispatcher.adrain_pending()
        self.assertEqual([0, 1], received)
        dropped = await self.dispatcher.ashutdown()
        self.assertEqual(0, dropped)
        self.assertTrue(self.dispatcher._closed)


class TestAutoScheduler(unittest.IsolatedAsyncioTestCase):
    def test_sync_background_scheduler_delivers_queued_messages(self):
        dispatcher = Dispatcher(auto_schedule=True)
        received = []
        dispatcher.map(
            "/knob",
            lambda address, v: received.append(v),
            rate=50,
            burst=5,
        )
        try:
            for value in range(3):
                dispatcher.call_handlers_for_packet(
                    msg("/knob", value), ("127.0.0.1", 9000)
                )
            # The scheduler thread must pump the queue without more packets.
            import time as _time

            deadline_time = _time.time() + 5
            while len(received) < 3 and _time.time() < deadline_time:
                _time.sleep(0.02)
            self.assertEqual([0, 1, 2], received)
        finally:
            dispatcher.shutdown()
        # Scheduler thread has terminated.
        self.assertIsNone(dispatcher._scheduler_thread)

    async def test_async_background_task_delivers_queued_messages(self):
        dispatcher = Dispatcher(auto_schedule=True)
        received = []

        async def handler(address, v):
            received.append(v)

        dispatcher.map("/knob", handler, rate=50, burst=5)
        try:
            for value in range(3):
                await dispatcher.async_call_handlers_for_packet(
                    msg("/knob", value), ("127.0.0.1", 9000)
                )
            loop = asyncio.get_running_loop()
            start = loop.time()
            while len(received) < 3 and loop.time() - start < 5:
                await asyncio.sleep(0.02)
            self.assertEqual([0, 1, 2], received)
        finally:
            await dispatcher.ashutdown()
        self.assertIsNone(dispatcher._async_task)


if __name__ == "__main__":
    unittest.main()
