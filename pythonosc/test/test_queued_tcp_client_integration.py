"""Integration tests for the queued async TCP client.

A real loopback TCP server with a deliberately slow (and optionally gated)
reader provides genuine socket-level backpressure, so no real OSC device or
network is involved.
"""

import asyncio
import socket
import struct
import unittest

from pythonosc import osc_message_builder, tcp_client
from pythonosc.osc_message import OscMessage
from pythonosc.osc_tcp_server import MODE_1_0, MODE_1_1
from pythonosc.parsing.framing import FramingParser
from pythonosc.send_queue import (
    SendQueueClosed,
    SendQueueFull,
    SendQueueFullPolicy,
)

TEST_TIMEOUT = 10.0


def make_message(seq: int, payload: int = 16) -> OscMessage:
    builder = osc_message_builder.OscMessageBuilder("/seq")
    builder.add_arg(seq, osc_message_builder.OscMessageBuilder.ARG_TYPE_INT)
    builder.add_arg(b"x" * payload, osc_message_builder.OscMessageBuilder.ARG_TYPE_BLOB)
    return builder.build()


class SlowReaderServer:
    """Loopback server that accepts one client and reads very slowly.

    ``gated`` blocks the reader task entirely until :meth:`release` is
    called, letting the sender fill both the kernel buffers and the
    client-side queue.
    """

    def __init__(self, mode: str, *, gated: bool, read_delay: float = 0.002):
        self.mode = mode
        self.gated = gated
        self.read_delay = read_delay
        self.received_dgrams: list[bytes] = []
        self._gate: asyncio.Event | None = None
        self._server: asyncio.Server | None = None
        self._received_event = asyncio.Event()
        self._client_writers: list[asyncio.StreamWriter] = []
        self._handler_tasks: list[asyncio.Task[None]] = []

    async def wait_for_messages(
        self, count: int, timeout: float = TEST_TIMEOUT
    ) -> None:
        """Wait until the reader has parsed and processed ``count`` datagrams."""

        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.received_dgrams) < count:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(
                    f"expected {count} messages, got {len(self.received_dgrams)}"
                )
            self._received_event.clear()
            try:
                await asyncio.wait_for(self._received_event.wait(), remaining)
            except asyncio.TimeoutError:
                pass

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> None:
        if self.gated:
            self._gate = asyncio.Event()
        self._server = await asyncio.start_server(
            self._handle_client, "127.0.0.1", 0, family=socket.AF_INET
        )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.create_task(self._handle(reader, writer))
        self._handler_tasks.append(task)
        try:
            await task
        except asyncio.CancelledError:
            raise

    def release(self) -> None:
        if self._gate is not None:
            self._gate.set()

    async def abort(self) -> None:
        """Hard-close accepted connections with RST to simulate a broken peer."""

        for writer in self._client_writers:
            sock = writer.transport.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                )
            writer.transport.abort()
        await self.stop()

    async def stop(self) -> None:
        assert self._server is not None
        self.release()
        for task in self._handler_tasks:
            task.cancel()
        for task in self._handler_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._handler_tasks.clear()
        self._server.close()
        await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._client_writers.append(writer)
        framing = tcp_client.get_framing(self.mode)
        # The backpressure tests intentionally push multi-megabyte frames.
        parser = FramingParser(framing, max_buffer_size=32 * 1024 * 1024)
        try:
            while chunk := await reader.read(4096):
                if self._gate is not None:
                    await self._gate.wait()
                for dgram in parser.feed(chunk):
                    self.received_dgrams.append(dgram)
                    self._received_event.set()
                    if self.read_delay:
                        await asyncio.sleep(self.read_delay)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


class QueuedClientIntegrationBase(unittest.IsolatedAsyncioTestCase):
    """Common scenarios, run once per framing mode via the concrete subclasses."""

    mode = MODE_1_1
    __test__ = False  # do not collect the base class itself

    async def asyncSetUp(self) -> None:
        self._bg_tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]

    def assert_no_stray_tasks(self) -> None:
        stray = [
            t
            for t in asyncio.all_tasks()
            if t not in self._bg_tasks
            and not t.done()
            and t is not asyncio.current_task()
        ]
        self.assertEqual(stray, [])

    @staticmethod
    def seq_of(dgram: bytes) -> int:
        return OscMessage(dgram).params[0]

    async def make_server(
        self, *, gated: bool, read_delay: float = 0.002
    ) -> SlowReaderServer:
        server = SlowReaderServer(self.mode, gated=gated, read_delay=read_delay)
        await server.start()
        return server

    async def test_all_frames_arrive_in_order_with_slow_reader(self) -> None:
        server = await self.make_server(gated=False)
        try:
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1", server.port, mode=self.mode, max_messages=50
            )
            await client.__open__()
            n = 60
            try:
                results = await asyncio.gather(
                    *[
                        asyncio.wait_for(client.send(make_message(i)), TEST_TIMEOUT)
                        for i in range(n)
                    ]
                )
                self.assertTrue(all(results))
            finally:
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)

            await server.wait_for_messages(n)
            self.assertEqual(len(server.received_dgrams), n)
            self.assertEqual(
                [self.seq_of(d) for d in server.received_dgrams], list(range(n))
            )
            # Every datagram decodes standalone: no truncation/interleaving.
            self.assertTrue(
                all(OscMessage(d).address == "/seq" for d in server.received_dgrams)
            )
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_concurrent_producers_preserve_per_producer_order(self) -> None:
        server = await self.make_server(gated=False)
        try:
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1", server.port, mode=self.mode, max_messages=10
            )
            await client.__open__()
            n_per_producer = 15
            total = 4 * n_per_producer
            try:

                async def producer(base: int) -> None:
                    for j in range(n_per_producer):
                        seq = base * n_per_producer + j
                        sent = await asyncio.wait_for(
                            client.send(make_message(seq)), TEST_TIMEOUT
                        )
                        self.assertTrue(sent)

                await asyncio.gather(*[producer(b) for b in range(4)])
            finally:
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)

            await server.wait_for_messages(total)
            self.assertEqual(len(server.received_dgrams), total)
            seqs = [self.seq_of(d) for d in server.received_dgrams]
            self.assertEqual(sorted(seqs), list(range(total)))
            # Within each producer, strict enqueue order was preserved.
            for base in range(4):
                producer_seqs = [s for s in seqs if s // n_per_producer == base]
                self.assertEqual(
                    producer_seqs,
                    [base * n_per_producer + j for j in range(n_per_producer)],
                )
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_error_policy_raises_once_message_bound_is_hit(self) -> None:
        server = await self.make_server(gated=True)
        try:
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1",
                server.port,
                mode=self.mode,
                max_messages=5,
                full_policy=SendQueueFullPolicy.ERROR,
            )
            await client.__open__()
            try:
                # Concurrent enqueues fill the queue while the gated writer
                # cannot make progress; one of them must hit the bound.
                # Independent tasks avoid gather cancelling the survivors.
                tasks = [
                    asyncio.create_task(
                        asyncio.wait_for(
                            client.send(make_message(i, payload=1024)),
                            TEST_TIMEOUT,
                        )
                    )
                    for i in range(8)
                ]
                while not any(t.done() and t.exception() for t in tasks):
                    await asyncio.sleep(0.01)
                failures = [t for t in tasks if t.done() and t.exception()]
                self.assertTrue(
                    any(isinstance(t.exception(), SendQueueFull) for t in failures)
                )
                # The bound is a hard ceiling, even while the writer is blocked.
                self.assertLessEqual(client._queue.qsize(), 5)

                # Releasing the gate drains the queue and sending works again.
                server.release()
                for _ in range(100):
                    if all(t.done() for t in tasks):
                        break
                    await asyncio.sleep(0.02)
                # Every non-rejected task reports a successful send.
                for task in tasks:
                    if task.done() and task.exception() is None:
                        self.assertTrue(task.result())
                self.assertEqual(client._queue.qsize(), 0)
                sent = await asyncio.wait_for(
                    client.send(make_message(999)), TEST_TIMEOUT
                )
                self.assertTrue(sent)
            finally:
                server.release()
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_byte_bound_enforced_against_framed_sizes(self) -> None:
        server = await self.make_server(gated=True)
        try:
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1",
                server.port,
                mode=self.mode,
                max_bytes=4096,
                full_policy=SendQueueFullPolicy.ERROR,
            )
            await client.__open__()
            try:
                # Gate keeps the writer blocked; queued bytes accumulate until
                # the byte bound rejects the next enqueue. Independent tasks
                # (not gather, which would cancel the survivors on the error).
                tasks = [
                    asyncio.create_task(
                        asyncio.wait_for(
                            client.send(make_message(i, payload=1024)),
                            TEST_TIMEOUT,
                        )
                    )
                    for i in range(40)
                ]
                while not any(
                    t.done() and not t.cancelled() and t.exception() for t in tasks
                ):
                    await asyncio.sleep(0.01)
                failures = [t for t in tasks if t.done() and t.exception()]
                self.assertTrue(
                    any(isinstance(t.exception(), SendQueueFull) for t in failures)
                )
                # The byte ceiling is never exceeded, even transiently.
                self.assertLessEqual(client._queue.size_bytes(), 4096)
            finally:
                server.release()
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_cancelling_waiting_send_neither_enqueues_nor_reorders(self) -> None:
        server = await self.make_server(gated=True)
        try:
            # Single-slot queue. The writer takes the first frame and blocks
            # in drain() (peer never reads), so the second frame occupies the
            # only slot and every further send is parked in enqueue().
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1", server.port, mode=self.mode, max_messages=1
            )
            await client.__open__()
            try:
                big = 5 * 1024 * 1024
                first = asyncio.create_task(client.send(make_message(1, big)))
                second = asyncio.create_task(client.send(make_message(2, big)))

                # Wait deterministically for the writer to be blocked with
                # the second frame filling the single queue slot.
                for _ in range(500):
                    if client._queue.qsize() == 1:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(client._queue.qsize(), 1)

                blocked = asyncio.create_task(client.send(make_message(3, big)))
                # Give the task a chance to run; with a full slot it must park.
                for _ in range(50):
                    await asyncio.sleep(0.01)
                    if not blocked.done() and client._queue.qsize() == 1:
                        break
                self.assertFalse(blocked.done())
                self.assertEqual(client._queue.qsize(), 1)

                blocked.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await blocked

                # The cancelled frame never entered the queue.
                self.assertEqual(client._queue.qsize(), 1)

                server.release()
                self.assertTrue(await asyncio.wait_for(first, TEST_TIMEOUT))
                self.assertTrue(await asyncio.wait_for(second, TEST_TIMEOUT))
                fourth = await asyncio.wait_for(
                    client.send(make_message(4)), TEST_TIMEOUT
                )
                self.assertTrue(fourth)
            finally:
                server.release()
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)

            await server.wait_for_messages(3)
            seqs = [self.seq_of(d) for d in server.received_dgrams]
            self.assertEqual(seqs, [1, 2, 4])
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_connection_reset_fails_queued_and_new_sends(self) -> None:
        # Ungated but very slow reader: messages remain in flight/queued.
        server = await self.make_server(gated=False, read_delay=0.05)
        client = tcp_client.QueuedAsyncTCPClient(
            "127.0.0.1", server.port, mode=self.mode, max_messages=50
        )
        await client.__open__()

        async def keep_sending() -> None:
            i = 0
            while True:
                await client.send(make_message(i, payload=4096))
                i += 1
                await asyncio.sleep(0)

        sender = asyncio.create_task(keep_sending())
        await asyncio.sleep(0.2)

        # RST the peer: the blocked drain must surface an OSError and the
        # queue must fail every pending and future sender.
        await server.abort()
        try:
            with self.assertRaises((OSError, SendQueueClosed)):
                await asyncio.wait_for(sender, TEST_TIMEOUT)

            with self.assertRaises((OSError, SendQueueClosed)):
                await asyncio.wait_for(client.send(make_message(123)), TEST_TIMEOUT)

            # Closing the client must not hang or raise.
            await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)
        finally:
            sender.cancel()
            # The sender may already have died with the connection error.
            try:
                await sender
            except (asyncio.CancelledError, OSError, SendQueueClosed):
                pass
        self.assert_no_stray_tasks()

    async def test_drain_close_flushes_every_message_in_order(self) -> None:
        server = await self.make_server(gated=False, read_delay=0.01)
        try:
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1", server.port, mode=self.mode, max_messages=10
            )
            await client.__open__()
            n = 30
            try:
                await asyncio.gather(
                    *[
                        asyncio.wait_for(client.send(make_message(i)), TEST_TIMEOUT)
                        for i in range(n)
                    ]
                )
                await asyncio.wait_for(client.close(drain=True), TEST_TIMEOUT)
            except BaseException:
                await client.close(drain=False)
                raise

            await server.wait_for_messages(n)
            seqs = [self.seq_of(d) for d in server.received_dgrams]
            self.assertEqual(seqs, list(range(n)))
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_discard_close_drops_pending_and_reports_false(self) -> None:
        server = await self.make_server(gated=True)
        try:
            # Tiny queue with big frames: the gated writer blocks in drain()
            # while the three slots fill, guaranteeing accepted-but-unsent
            # messages alongside producers still waiting to enqueue. A short
            # close timeout keeps teardown fast while the gated peer is not
            # reading.
            client = tcp_client.QueuedAsyncTCPClient(
                "127.0.0.1",
                server.port,
                mode=self.mode,
                max_messages=3,
                close_timeout=0.5,
            )
            await client.__open__()
            try:
                pending = [
                    asyncio.ensure_future(client.send(make_message(i, payload=1 << 20)))
                    for i in range(10)
                ]
                for _ in range(500):
                    if client._queue.qsize() >= 3:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(client._queue.qsize(), 3)
                await asyncio.wait_for(client.close(drain=False), TEST_TIMEOUT)

                results = await asyncio.gather(*pending, return_exceptions=True)
                # Every sender is resolved: none hangs, none left pending.
                accepted = 0
                discarded = 0
                rejected = 0
                for result in results:
                    if result is True:
                        accepted += 1  # already handed to the transport
                    elif result is False:
                        discarded += 1  # enqueued then dropped by close()
                    elif isinstance(result, SendQueueClosed):
                        rejected += 1  # still waiting when shutdown began
                    else:
                        raise AssertionError(f"unexpected send outcome: {result!r}")
                self.assertGreater(discarded, 0)
                self.assertEqual(accepted + discarded + rejected, len(pending))

                # Sends after shutdown are rejected.
                with self.assertRaises(SendQueueClosed):
                    await client.send(make_message(100))
            finally:
                server.release()
        finally:
            await server.stop()
        self.assert_no_stray_tasks()

    async def test_async_context_manager_drains_on_exit(self) -> None:
        server = await self.make_server(gated=False, read_delay=0.005)
        try:
            async with tcp_client.QueuedAsyncSimpleTCPClient(
                "127.0.0.1", server.port, mode=self.mode, max_messages=8
            ) as client:
                for i in range(20):
                    await asyncio.wait_for(
                        client.send_message("/seq", [i, b"x" * 64]), TEST_TIMEOUT
                    )

            await server.wait_for_messages(20)
            seqs = [OscMessage(d).params[0] for d in server.received_dgrams]
            self.assertEqual(seqs, list(range(20)))
        finally:
            await server.stop()
        self.assert_no_stray_tasks()


class TestSlipMode(QueuedClientIntegrationBase):
    mode = MODE_1_1
    __test__ = True


class TestBinaryLengthMode(QueuedClientIntegrationBase):
    mode = MODE_1_0
    __test__ = True


if __name__ == "__main__":
    unittest.main()
