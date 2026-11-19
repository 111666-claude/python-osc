import threading

import unittest

from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import OscMessageBuilder
from pythonosc.rate_limit import (
    OverflowPolicy,
    RateLimitError,
    RouteRateLimiter,
    SubmitResult,
    TokenBucket,
)


class FakeClock:
    """Deterministic, manually advanced clock for rate limiter tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.time = float(start)

    def __call__(self) -> float:
        return self.time

    def advance(self, seconds: float) -> None:
        self.time += seconds

    def rollback(self, seconds: float) -> None:
        self.time -= seconds


def make_message(address: str = "/knob", value: int = 0) -> OscMessage:
    builder = OscMessageBuilder(address)
    builder.add_arg(value)
    return builder.build()


class TestTokenBucket(unittest.TestCase):
    def test_burst_is_available_immediately(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=2.0, burst=3.0, clock=clock)
        for _ in range(3):
            self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())

    def test_refills_at_configured_rate(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=10.0, burst=2.0, clock=clock)
        self.assertTrue(bucket.take())
        self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())
        # Half a second at 10/s accrues 5 tokens, but the cap is 2.
        clock.advance(0.5)
        self.assertTrue(bucket.take())
        self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())

    def test_partial_refill_accrues_fractional_tokens(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=4.0, burst=2.0, clock=clock)
        bucket.take()
        bucket.take()
        clock.advance(0.25)  # one token accrued
        self.assertTrue(bucket.take())
        self.assertFalse(bucket.take())
        clock.advance(0.25)
        self.assertTrue(bucket.take())

    def test_clock_rollback_grants_no_tokens(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=10.0, burst=5.0, clock=clock)
        bucket.take()
        bucket.take()
        clock.rollback(50.0)
        # A backwards jump must not look like accrued time.
        self.assertAlmostEqual(bucket.tokens, 3.0)
        clock.advance(0.1)  # only 1 token after the new anchor
        self.assertAlmostEqual(bucket.tokens, 4.0)
        self.assertTrue(bucket.take())

    def test_invalid_parameters_rejected(self):
        clock = FakeClock()
        with self.assertRaises(RateLimitError):
            TokenBucket(rate=0, burst=1, clock=clock)
        with self.assertRaises(RateLimitError):
            TokenBucket(rate=1, burst=0, clock=clock)
        with self.assertRaises(RateLimitError):
            TokenBucket(rate=-2, burst=1, clock=clock)

    def test_reconfigure_preserves_tokens_within_new_burst(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=10.0, burst=5.0, clock=clock)
        for _ in range(4):
            bucket.take()
        bucket.reconfigure(rate=1.0, burst=2.0)
        self.assertAlmostEqual(bucket.tokens, 1.0)
        clock.advance(1.0)
        self.assertAlmostEqual(bucket.tokens, 2.0)

    def test_refund_capped_at_burst(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=1.0, burst=2.0, clock=clock)
        bucket.refund()
        bucket.refund()
        self.assertAlmostEqual(bucket.tokens, 2.0)


class TestRouteRateLimiter(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()

    def test_burst_delivered_then_queue(self):
        limiter = RouteRateLimiter(
            rate=2.0, burst=2.0, clock=self.clock
        )
        msg = make_message()
        self.assertEqual(
            SubmitResult.DELIVERED, limiter.submit(("1.2.3.4", 1), msg, self.clock.time, [])
        )
        self.assertEqual(
            SubmitResult.DELIVERED, limiter.submit(("1.2.3.4", 1), msg, self.clock.time, [])
        )
        self.assertEqual(
            SubmitResult.QUEUED, limiter.submit(("1.2.3.4", 1), msg, self.clock.time, [])
        )
        self.assertEqual(1, limiter.queue_size)

    def test_drop_oldest_keeps_latest_value(self):
        limiter = RouteRateLimiter(
            rate=1.0,
            burst=1.0,
            clock=self.clock,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        first = make_message(value=1)
        second = make_message(value=2)
        third = make_message(value=3)
        self.assertEqual(
            SubmitResult.DELIVERED,
            limiter.submit(("1.2.3.4", 1), first, self.clock.time, ["h1"]),
        )
        self.assertEqual(
            SubmitResult.QUEUED,
            limiter.submit(("1.2.3.4", 1), second, self.clock.time, ["h2"]),
        )
        # Queue holds exactly one message: the newcomer evicts the old one.
        self.assertEqual(
            SubmitResult.QUEUED,
            limiter.submit(("1.2.3.4", 1), third, self.clock.time, ["h3"]),
        )
        self.assertEqual(1, limiter.stats.dropped_old)
        self.clock.advance(1.0)
        due = limiter.pump_due()
        self.assertEqual(1, len(due))
        self.assertIs(third, due[0].message)
        self.assertEqual(["h3"], due[0].handlers)

    def test_reject_new_preserves_queued_values(self):
        limiter = RouteRateLimiter(
            rate=1.0,
            burst=1.0,
            clock=self.clock,
            overflow_policy=OverflowPolicy.REJECT_NEW,
        )
        first = make_message(value=1)
        second = make_message(value=2)
        third = make_message(value=3)
        self.assertEqual(
            SubmitResult.DELIVERED,
            limiter.submit(("1.2.3.4", 1), first, self.clock.time, []),
        )
        self.assertEqual(
            SubmitResult.QUEUED,
            limiter.submit(("1.2.3.4", 1), second, self.clock.time, []),
        )
        self.assertEqual(
            SubmitResult.REJECTED,
            limiter.submit(("1.2.3.4", 1), third, self.clock.time, []),
        )
        self.assertEqual(1, limiter.stats.rejected)
        self.clock.advance(1.0)
        due = limiter.pump_due()
        self.assertEqual(1, len(due))
        self.assertIs(second, due[0].message)

    def test_pump_releases_in_fifo_order_at_refill_rate(self):
        # Burst of 2: the first two messages deliver inline and the queue
        # (capacity = burst) holds the next two, whose FIFO release order is
        # then observable. Rate of 1/s keeps virtual-clock math on seconds.
        limiter = RouteRateLimiter(
            rate=1.0, burst=2.0, clock=self.clock
        )
        for value in range(1, 5):
            limiter.submit(
                ("1.2.3.4", 1), make_message(value=value), self.clock.time, []
            )
        self.assertEqual(2, limiter.queue_size)
        self.assertEqual([], limiter.pump_due())
        self.clock.advance(1)
        released = limiter.pump_due()
        self.assertEqual([3], [item.message.params[0] for item in released])
        self.clock.advance(1)
        released = limiter.pump_due()
        self.assertEqual([4], [item.message.params[0] for item in released])
        self.assertEqual(0, limiter.queue_size)

    def test_rollback_queued_and_delivered_submissions(self):
        limiter = RouteRateLimiter(
            rate=1.0, burst=1.0, clock=self.clock
        )
        first = make_message(value=1)
        second = make_message(value=2)
        delivered = limiter.submit(("1.2.3.4", 1), first, self.clock.time, [])
        queued = limiter.submit(("1.2.3.4", 1), second, self.clock.time, [])
        self.assertEqual(SubmitResult.DELIVERED, delivered)
        self.assertEqual(SubmitResult.QUEUED, queued)
        limiter.rollback_submission(second, queued)
        self.assertEqual(0, limiter.queue_size)
        limiter.rollback_submission(first, delivered)
        # Token restored: a new submission delivers immediately.
        self.assertEqual(
            SubmitResult.DELIVERED,
            limiter.submit(("1.2.3.4", 1), make_message(value=9), self.clock.time, []),
        )

    def test_closed_limiter_rejects_everything(self):
        limiter = RouteRateLimiter(
            rate=1.0, burst=2.0, clock=self.clock
        )
        limiter.submit(("1.2.3.4", 1), make_message(value=1), self.clock.time, [])
        limiter.submit(("1.2.3.4", 1), make_message(value=2), self.clock.time, [])
        limiter.submit(("1.2.3.4", 1), make_message(value=3), self.clock.time, [])
        # Burst of 2: two delivered, one queued (queue capacity = burst).
        dropped = limiter.close()
        self.assertEqual(1, dropped)
        self.assertTrue(limiter.closed)
        result = limiter.submit(
            ("1.2.3.4", 1), make_message(value=4), self.clock.time, []
        )
        self.assertEqual(SubmitResult.REJECTED, result)
        self.assertEqual(1, limiter.stats.rejected)

    def test_drain_all_returns_everything_without_tokens(self):
        limiter = RouteRateLimiter(
            rate=1.0, burst=1.0, clock=self.clock
        )
        limiter.submit(("1.2.3.4", 1), make_message(value=1), self.clock.time, [])
        limiter.submit(("1.2.3.4", 1), make_message(value=2), self.clock.time, [])
        items = limiter.drain_all()
        self.assertEqual([2], [item.message.params[0] for item in items])
        self.assertEqual(0, limiter.queue_size)

    def test_next_wakeup_matches_token_refill(self):
        limiter = RouteRateLimiter(
            rate=4.0, burst=1.0, clock=self.clock
        )
        self.assertIsNone(limiter.next_wakeup())
        limiter.submit(("1.2.3.4", 1), make_message(), self.clock.time, [])
        limiter.submit(("1.2.3.4", 1), make_message(), self.clock.time, [])
        self.assertAlmostEqual(self.clock.time + 0.25, limiter.next_wakeup())

    def test_thread_safety_under_contention(self):
        limiter = RouteRateLimiter(
            rate=1000.0, burst=10.0, clock=self.clock
        )

        def worker():
            for _ in range(500):
                limiter.submit(
                    ("1.2.3.4", 1), make_message(), self.clock.time, []
                )

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        stats = limiter.stats
        self.assertEqual(2000, stats.submitted)
        # Every submission is accounted for exactly once: a message is either
        # delivered, still queued, rejected, or evicted as an old value.
        self.assertEqual(
            stats.submitted,
            stats.accounted + stats.rejected + stats.dropped_old,
        )
        # The bounded queue never exceeds its burst-derived capacity.
        self.assertLessEqual(limiter.queue_size, limiter.capacity)
        self.assertEqual(stats.queued, limiter.queue_size)


if __name__ == "__main__":
    unittest.main()
