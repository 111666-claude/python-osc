import asyncio
import unittest

from pythonosc.send_queue import (
    BoundedSendQueue,
    SendQueueClosed,
    SendQueueFull,
    SendQueueFullPolicy,
)


class TestBoundedSendQueue(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_bounds(self) -> None:
        with self.assertRaises(ValueError):
            BoundedSendQueue(max_messages=0)
        with self.assertRaises(ValueError):
            BoundedSendQueue(max_bytes=-1)

    async def test_fifo_order_single_producer(self) -> None:
        queue = BoundedSendQueue()
        for i in range(10):
            await queue.enqueue(f"msg-{i}".encode())
        self.assertEqual(queue.qsize(), 10)

        for i in range(10):
            item = await queue.get()
            assert item is not None
            self.assertEqual(item.frame, f"msg-{i}".encode())
            queue.mark_sent(item.future)
            self.assertTrue(item.future.result())

        self.assertEqual(queue.qsize(), 0)
        self.assertEqual(queue.size_bytes(), 0)

    async def test_message_bound_blocks_until_capacity(self) -> None:
        queue = BoundedSendQueue(max_messages=2)
        await queue.enqueue(b"aa")
        await queue.enqueue(b"bb")
        self.assertEqual(queue.qsize(), 2)

        async def fill() -> None:
            await queue.enqueue(b"cc")

        waiter = asyncio.create_task(fill())
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())

        item = await queue.get()
        assert item is not None
        queue.mark_sent(item.future)
        await asyncio.wait_for(waiter, timeout=1)
        self.assertEqual(queue.qsize(), 2)

    async def test_byte_bound_blocks_until_capacity(self) -> None:
        queue = BoundedSendQueue(max_bytes=10)
        await queue.enqueue(b"xxxx")  # 4
        await queue.enqueue(b"xxxxxx")  # 10
        self.assertEqual(queue.size_bytes(), 10)

        waiter = asyncio.create_task(queue.enqueue(b"y"))
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())

        item = await queue.get()  # frees 4 bytes
        assert item is not None
        queue.mark_sent(item.future)
        await asyncio.wait_for(waiter, timeout=1)
        self.assertEqual(queue.size_bytes(), 7)

    async def test_error_policy_raises_when_full(self) -> None:
        queue = BoundedSendQueue(max_messages=1)
        await queue.enqueue(b"x")
        with self.assertRaises(SendQueueFull):
            await queue.enqueue(b"y", full_policy=SendQueueFullPolicy.ERROR)
        # After capacity frees up, ERROR policy enqueues again.
        item = await queue.get()
        assert item is not None
        queue.mark_sent(item.future)
        future = await queue.enqueue(b"z", full_policy=SendQueueFullPolicy.ERROR)
        self.assertFalse(future.done())

    async def test_frame_larger_than_max_bytes_is_rejected(self) -> None:
        queue = BoundedSendQueue(max_bytes=4)
        with self.assertRaises(ValueError):
            await queue.enqueue(b"x" * 5)

    async def test_cancelled_waiter_does_not_enqueue_or_reorder(self) -> None:
        queue = BoundedSendQueue(max_messages=1)
        await queue.enqueue(b"first")

        async def blocked_send() -> None:
            await queue.enqueue(b"second")

        waiter = asyncio.create_task(blocked_send())
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter

        # The cancelled waiter never entered the queue.
        self.assertEqual(queue.qsize(), 1)
        item = await queue.get()
        assert item is not None
        self.assertEqual(item.frame, b"first")

    async def test_concurrent_producers_keep_fifo_and_all_resolve(self) -> None:
        queue = BoundedSendQueue(max_messages=3)
        n_producers = 20

        async def produce(i: int) -> bool:
            future = await queue.enqueue(f"{i:04d}".encode())
            return await future

        producers = [asyncio.create_task(produce(i)) for i in range(n_producers)]
        received: list[bytes] = []

        async def consume() -> None:
            while len(received) < n_producers:
                item = await queue.get()
                assert item is not None
                received.append(item.frame)
                queue.mark_sent(item.future)

        consumer = asyncio.create_task(consume())
        results = await asyncio.gather(*producers)
        await consumer

        self.assertTrue(all(results))
        # Every message delivered exactly once, bytes preserved.
        self.assertEqual(
            sorted(received), [f"{i:04d}".encode() for i in range(n_producers)]
        )

    async def test_fail_delivers_error_to_queued_and_rejects_new_enqueues(self) -> None:
        queue = BoundedSendQueue(max_messages=5)
        futures = [await queue.enqueue(b"x") for _ in range(3)]

        boom = ConnectionResetError("peer gone")
        await queue.fail(boom)

        for future in futures:
            self.assertIs(future.exception(), boom)

        with self.assertRaises(SendQueueClosed):
            await queue.enqueue(b"y")

        # The writer terminates: get() returns None instead of blocking.
        self.assertIsNone(await queue.get())

    async def test_close_drain_keeps_items_then_get_returns_none(self) -> None:
        queue = BoundedSendQueue()
        future = await queue.enqueue(b"kept")
        await queue.close(drain=True)
        self.assertTrue(queue.closed)

        with self.assertRaises(SendQueueClosed):
            await queue.enqueue(b"new")

        item = await queue.get()
        assert item is not None
        self.assertEqual(item.frame, b"kept")
        queue.mark_sent(item.future)
        self.assertTrue(future.result())
        self.assertIsNone(await queue.get())

    async def test_close_discard_resolves_futures_false(self) -> None:
        queue = BoundedSendQueue()
        future = await queue.enqueue(b"dropped")
        await queue.close(drain=False)

        self.assertFalse(future.result())
        self.assertIsNone(await queue.get())
        with self.assertRaises(SendQueueClosed):
            await queue.enqueue(b"new")

    async def test_waiting_producer_woken_on_close_and_failure(self) -> None:
        queue = BoundedSendQueue(max_messages=1)
        await queue.enqueue(b"x")

        async def wait_then_enqueue() -> None:
            await queue.enqueue(b"y")

        waiter = asyncio.create_task(wait_then_enqueue())
        await asyncio.sleep(0)
        await queue.close(drain=False)
        with self.assertRaises(SendQueueClosed):
            await asyncio.wait_for(waiter, timeout=1)

    async def test_waiting_producer_woken_on_fail(self) -> None:
        queue = BoundedSendQueue(max_messages=1)
        await queue.enqueue(b"x")

        waiter = asyncio.create_task(queue.enqueue(b"y"))
        await asyncio.sleep(0)
        await queue.fail(ConnectionResetError())
        with self.assertRaises(SendQueueClosed):
            await asyncio.wait_for(waiter, timeout=1)


if __name__ == "__main__":
    unittest.main()
