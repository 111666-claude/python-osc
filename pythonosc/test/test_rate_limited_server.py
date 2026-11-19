"""Integration tests for rate limiting through real (loopback) servers."""

import asyncio
import socket
import threading
import time
import unittest

from pythonosc import dispatcher, osc_server
from pythonosc.rate_limit import OverflowPolicy
from pythonosc.udp_client import SimpleUDPClient


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class TestRateLimitedUDPServer(unittest.TestCase):
    def test_blocking_server_throttles_and_shuts_down(self):
        port = free_port()
        received = []
        dispatch = dispatcher.Dispatcher()
        dispatch.map(
            "/knob",
            lambda address, value: received.append(value),
            rate=30,
            burst=2,
        )
        server = osc_server.BlockingOSCUDPServer(("127.0.0.1", port), dispatch)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = SimpleUDPClient("127.0.0.1", port)
            for value in range(20):
                client.send_message("/knob", value)
            # Give the background scheduler time to pump the queue.
            deadline = time.time() + 3
            while time.time() < deadline and len(received) < 4:
                time.sleep(0.02)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        stats = dispatch.rate_limit_stats("/knob")
        self.assertEqual(20, stats.submitted)
        # DROP_OLDEST: far fewer than 20 handler calls, latest values kept,
        # everything delivered in order.
        self.assertLess(len(received), 20)
        self.assertEqual(received, sorted(received))
        self.assertGreater(stats.dropped_old, 0)
        # server_close deterministically stopped rate limit scheduling.
        self.assertTrue(dispatch._closed)
        self.assertIsNone(dispatch._scheduler_thread)

    def test_blocking_server_reject_new_policy(self):
        port = free_port()
        received = []
        dispatch = dispatcher.Dispatcher()
        dispatch.map(
            "/knob",
            lambda address, value: received.append(value),
            rate=30,
            burst=1,
            overflow_policy=OverflowPolicy.REJECT_NEW,
        )
        server = osc_server.BlockingOSCUDPServer(("127.0.0.1", port), dispatch)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = SimpleUDPClient("127.0.0.1", port)
            for value in range(10):
                client.send_message("/knob", value)
            time.sleep(0.5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        stats = dispatch.rate_limit_stats("/knob")
        self.assertGreater(stats.rejected, 0)
        self.assertEqual(0, stats.dropped_old)
        # Rejected values are never delivered; queued ones are FIFO.
        self.assertEqual(received, sorted(received))


class TestRateLimitedAsyncUDPServer(unittest.IsolatedAsyncioTestCase):
    async def test_async_server_throttles_and_closes(self):
        port = free_port()
        received = []

        def handler(address: str, value: int) -> None:
            received.append(value)

        dispatch = dispatcher.Dispatcher()
        dispatch.map("/knob", handler, rate=30, burst=2)
        loop = asyncio.get_running_loop()
        server = osc_server.AsyncIOOSCUDPServer(
            ("127.0.0.1", port), dispatch, loop
        )
        transport, _protocol = await server.create_serve_endpoint()
        try:
            client = SimpleUDPClient("127.0.0.1", port)
            for value in range(20):
                client.send_message("/knob", value)
            deadline = loop.time() + 3
            while loop.time() < deadline and len(received) < 4:
                await asyncio.sleep(0.02)
        finally:
            dropped = await server.aclose()
            transport.close()

        self.assertIsInstance(dropped, int)
        self.assertTrue(dispatch._closed)
        self.assertIsNone(dispatch._async_task)
        stats = dispatch.rate_limit_stats("/knob")
        self.assertEqual(20, stats.submitted)
        self.assertEqual(received, sorted(received))


if __name__ == "__main__":
    unittest.main()
