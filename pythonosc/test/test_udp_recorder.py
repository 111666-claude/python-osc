import asyncio
import socket
import threading
import time
import unittest
from typing import cast
from unittest import mock

from pythonosc import (
    dispatcher,
    osc_bundle_builder,
    osc_message_builder,
    osc_server,
    udp_client,
)
from pythonosc.osc_message import OscMessage
from pythonosc.udp_recorder import (
    CapacityOverflowError,
    ClockRegressionError,
    DatagramPlayer,
    DatagramRecorder,
    InvalidRateError,
    OverflowPolicy,
    PlaybackError,
    RecordingTrack,
)

_SIMPLE_PARAM_INT_MSG = b"/SYNC\x00\x00\x00,i\x00\x00\x00\x00\x00\x04"
_SIMPLE_MSG_NO_PARAMS = b"/SYNC\x00\x00\x00"
_SECOND_MSG = b"/OTHER\x00\x00,i\x00\x00\x00\x00\x00\x07"
_BAD_DATAGRAM = b"this is not osc at all"


class FakeClock:
    """Deterministic monotonic stand-in that only moves when told to."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestDatagramRecorder(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.recorder = DatagramRecorder(clock=self.clock)

    def test_records_raw_bytes_untouched_and_relative_times(self) -> None:
        self.recorder.start()
        self.clock.advance(0.5)
        first = self.recorder.record(_SIMPLE_PARAM_INT_MSG)
        self.clock.advance(1.5)
        second = self.recorder.record(_SECOND_MSG)

        assert first is not None and second is not None
        self.assertEqual(first.relative_time, 0.5)
        self.assertEqual(second.relative_time, 2.0)
        track = self.recorder.track
        self.assertEqual(len(track), 2)
        self.assertEqual(track[0].datagram, _SIMPLE_PARAM_INT_MSG)
        self.assertEqual(track[1].datagram, _SECOND_MSG)
        self.assertEqual([p.sequence for p in track], [0, 1])

    def test_preserves_bundle_bytes_including_timetag(self) -> None:
        nested_builder = osc_message_builder.OscMessageBuilder("/nested")
        nested_builder.add_arg(1)
        deep_builder = osc_message_builder.OscMessageBuilder("/deep")
        deep_builder.add_arg(2)
        bundle = osc_bundle_builder.OscBundleBuilder(12345678)
        bundle.add_content(nested_builder.build())
        inner = osc_bundle_builder.OscBundleBuilder(54321123)
        inner.add_content(deep_builder.build())
        bundle.add_content(inner.build())
        raw = bundle.build().dgram

        self.recorder.record(raw)
        self.assertEqual(self.recorder.track[0].datagram, raw)

    def test_auto_starts_on_first_record_and_pins_origin(self) -> None:
        self.clock.advance(3.0)
        packet = self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.assertTrue(self.recorder.is_recording)
        assert packet is not None
        self.assertEqual(packet.relative_time, 0.0)
        self.clock.advance(1.0)
        second = self.recorder.record(_SECOND_MSG)
        assert second is not None
        self.assertEqual(second.relative_time, 1.0)

    def test_relative_times_are_never_negative_on_real_clock(self) -> None:
        recorder = DatagramRecorder()
        for _ in range(25):
            recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.assertTrue(all(p.relative_time >= 0.0 for p in recorder.track))

    def test_clock_regression_raises_and_keeps_track_intact(self) -> None:
        self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.clock.now -= 1.0
        with self.assertRaises(ClockRegressionError):
            self.recorder.record(_SECOND_MSG)
        self.assertEqual(self.recorder.recorded_count, 1)
        self.clock.advance(2.0)
        self.assertEqual(self.recorder.recorded_count, 1)
        self.recorder.record(_SECOND_MSG)
        self.assertEqual(self.recorder.recorded_count, 2)

    def test_equal_clock_readings_are_not_regressions(self) -> None:
        self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.recorder.record(_SECOND_MSG)
        self.assertEqual(self.recorder.recorded_count, 2)

    def test_empty_and_non_bytes_datagrams_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.recorder.record(b"")
        with self.assertRaises(TypeError):
            self.recorder.record("not bytes")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.recorder.record(memoryview(b"x"))  # type: ignore[arg-type]
        self.assertEqual(self.recorder.recorded_count, 0)

    def test_invalid_capacity(self) -> None:
        with self.assertRaises(ValueError):
            DatagramRecorder(max_packets=0)
        with self.assertRaises(TypeError):
            DatagramRecorder(max_packets=1.5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            DatagramRecorder(max_packets=True)
        with self.assertRaises(TypeError):
            DatagramRecorder(overflow_policy="drop")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            DatagramRecorder(clock="now")  # type: ignore[arg-type]

    def test_overflow_drop_oldest(self) -> None:
        recorder = DatagramRecorder(
            max_packets=2, overflow_policy=OverflowPolicy.DROP_OLDEST, clock=self.clock
        )
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.clock.advance(1.0)
        recorder.record(_SIMPLE_PARAM_INT_MSG)
        self.clock.advance(1.0)
        dropped = recorder.record(_SECOND_MSG)

        assert dropped is not None
        self.assertEqual(dropped.datagram, _SECOND_MSG)
        self.assertEqual(recorder.dropped_packets, 1)
        track = recorder.track
        self.assertEqual(
            [p.datagram for p in track],
            [_SIMPLE_PARAM_INT_MSG, _SECOND_MSG],
        )
        # Original arrival order survives via the sequence numbers.
        self.assertEqual([p.sequence for p in track], [1, 2])

    def test_overflow_drop_newest(self) -> None:
        recorder = DatagramRecorder(
            max_packets=2, overflow_policy=OverflowPolicy.DROP_NEWEST, clock=self.clock
        )
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        recorder.record(_SIMPLE_PARAM_INT_MSG)
        self.assertIsNone(recorder.record(_SECOND_MSG))
        self.assertEqual(recorder.dropped_packets, 1)
        self.assertEqual(
            [p.datagram for p in recorder.track],
            [_SIMPLE_MSG_NO_PARAMS, _SIMPLE_PARAM_INT_MSG],
        )

    def test_overflow_raise(self) -> None:
        recorder = DatagramRecorder(
            max_packets=1, overflow_policy=OverflowPolicy.RAISE, clock=self.clock
        )
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        with self.assertRaises(CapacityOverflowError):
            recorder.record(_SECOND_MSG)
        self.assertEqual(recorder.dropped_packets, 0)
        self.assertEqual(recorder.track[0].datagram, _SIMPLE_MSG_NO_PARAMS)

    def test_stop_and_reset_lifecycle(self) -> None:
        self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.recorder.stop()
        self.assertFalse(self.recorder.is_recording)
        with self.assertRaises(Exception):
            self.recorder.record(_SECOND_MSG)
        # Stopping keeps the captured data available for replay.
        self.assertEqual(len(self.recorder.track), 1)
        self.recorder.reset()
        self.assertTrue(self.recorder.track.is_empty)
        self.assertEqual(self.recorder.dropped_packets, 0)
        self.recorder.record(_SECOND_MSG)
        self.assertTrue(self.recorder.is_recording)

    def test_start_twice_is_idempotent_and_start_after_stop_fails(self) -> None:
        self.recorder.start()
        self.clock.advance(5.0)
        self.recorder.start()  # must not move the pinned origin
        self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.assertEqual(self.recorder.track[0].relative_time, 5.0)
        self.recorder.stop()
        with self.assertRaises(Exception):
            self.recorder.start()

    def test_context_manager_stops_recorder(self) -> None:
        with DatagramRecorder(clock=self.clock) as recorder:
            recorder.record(_SIMPLE_MSG_NO_PARAMS)
        self.assertFalse(recorder.is_recording)

    def test_track_is_a_snapshot_independent_of_later_recording(self) -> None:
        self.recorder.record(_SIMPLE_MSG_NO_PARAMS)
        snapshot = self.recorder.track
        self.clock.advance(1.0)
        self.recorder.record(_SECOND_MSG)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(len(self.recorder.track), 2)
        self.assertEqual(snapshot, RecordingTrack(snapshot.packets))


class TestDatagramPlayer(unittest.TestCase):
    def test_invalid_rates_are_rejected(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        for bad in (0, -1.0, float("inf"), float("-inf"), float("nan"), "1", True):
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidRateError):
                    DatagramPlayer(client, rate=bad)  # type: ignore[arg-type]

    def test_scheduled_delays_apply_rate(self) -> None:
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(2.0)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        clock.advance(4.0)
        recorder.record(_SECOND_MSG)

        self.assertEqual(
            DatagramPlayer.scheduled_delays(recorder.track, 1.0), [2.0, 4.0]
        )
        self.assertEqual(
            DatagramPlayer.scheduled_delays(recorder.track, 2.0), [1.0, 2.0]
        )
        self.assertEqual(
            DatagramPlayer.scheduled_delays(recorder.track, 0.5), [4.0, 8.0]
        )
        with self.assertRaises(InvalidRateError):
            DatagramPlayer.scheduled_delays(recorder.track, 0)

    def test_empty_track_sends_nothing(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        player = DatagramPlayer(client)
        self.assertEqual(player.play(RecordingTrack()), 0)
        client.send_bytes.assert_not_called()
        self.assertFalse(player.is_playing)
        player.close()

    def test_blocking_play_sends_every_datagram_in_order(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(0.01)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        clock.advance(0.02)
        recorder.record(_SECOND_MSG)

        player = DatagramPlayer(client, rate=10.0)
        start = time.monotonic()
        sent = player.play(recorder.track)
        elapsed = time.monotonic() - start

        self.assertEqual(sent, 2)
        self.assertEqual(player.sent_packets, 2)
        self.assertEqual(
            client.send_bytes.call_args_list,
            [mock.call(_SIMPLE_MSG_NO_PARAMS), mock.call(_SECOND_MSG)],
        )
        # 0.03 recorded seconds at 10x -> ~3ms total; generous CI bound.
        self.assertLess(elapsed, 1.0)
        player.close()

    def test_playback_intervals_match_rate_on_real_clock(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(0.08)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        clock.advance(0.16)
        recorder.record(_SECOND_MSG)

        arrival: list[float] = []
        client.send_bytes.side_effect = lambda _data: arrival.append(
            time.monotonic()
        )
        player = DatagramPlayer(client, rate=2.0)
        player.play(recorder.track)
        player.close()

        self.assertEqual(len(arrival), 2)
        gap = arrival[1] - arrival[0]
        # 0.16s recorded at 2x -> ~80ms between packets.
        self.assertGreater(gap, 0.05)
        self.assertLess(gap, 0.3)

    def test_cancel_skips_unsent_packets(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(10.0)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        clock.advance(10.0)
        recorder.record(_SECOND_MSG)

        player = DatagramPlayer(client, rate=1.0)
        player.play(recorder.track, block=False)
        time.sleep(0.2)
        player.cancel()
        time.sleep(0.2)

        self.assertFalse(player.is_playing)
        client.send_bytes.assert_not_called()
        self.assertEqual(player.sent_packets, 0)
        self.assertEqual(player.cancelled_packets, 2)
        player.close()

    def test_cancel_after_some_sends_only_skips_remainder(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        recorder.record(_SIMPLE_MSG_NO_PARAMS)  # first delay is zero
        clock.advance(10.0)
        recorder.record(_SECOND_MSG)

        player = DatagramPlayer(client, rate=1.0)
        player.play(recorder.track, block=False)
        deadline = time.monotonic() + 1.0
        while not client.send_bytes.called and time.monotonic() < deadline:
            time.sleep(0.01)
        player.cancel()
        player.wait()

        client.send_bytes.assert_called_once_with(_SIMPLE_MSG_NO_PARAMS)
        self.assertEqual(player.sent_packets, 1)
        self.assertEqual(player.cancelled_packets, 1)
        player.close()

    def test_cancel_before_play_is_cleared_by_play(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        player = DatagramPlayer(client)
        player.cancel()
        self.assertEqual(player.play(recorder.track), 1)
        player.close()

    def test_close_joins_thread_and_leaves_nothing_running(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(5.0)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)

        player = DatagramPlayer(client)
        player.play(recorder.track, block=False)
        thread = player._thread
        assert thread is not None
        player.close()
        self.assertFalse(thread.is_alive())

    def test_context_manager_closes_player(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        with DatagramPlayer(client) as player:
            player.play(recorder.track)
        self.assertFalse(player.is_playing)

    def test_send_failure_surfaces_to_blocking_caller(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        client.send_bytes.side_effect = OSError("socket is gone")
        recorder = DatagramRecorder()
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        player = DatagramPlayer(client)
        with self.assertRaises(OSError):
            player.play(recorder.track)
        player.close()

    def test_send_failure_surfaces_from_wait_for_async_play(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        client.send_bytes.side_effect = OSError("nope")
        recorder = DatagramRecorder()
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        player = DatagramPlayer(client)
        player.play(recorder.track, block=False)
        with self.assertRaises(OSError):
            player.wait()
        player.close()

    def test_concurrent_play_is_rejected(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(30.0)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        player = DatagramPlayer(client)
        player.play(recorder.track, block=False)
        try:
            with self.assertRaises(PlaybackError):
                player.play(recorder.track)
        finally:
            player.close()

    def test_wait_timeout_raises(self) -> None:
        client = mock.Mock(spec=udp_client.UDPClient)
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        clock.advance(30.0)
        recorder.record(_SIMPLE_MSG_NO_PARAMS)
        player = DatagramPlayer(client)
        player.play(recorder.track, block=False)
        try:
            with self.assertRaises(TimeoutError):
                player.wait(timeout=0.05)
        finally:
            player.close()

    def test_slow_receiver_does_not_queue_packets(self) -> None:
        # A blocking send holds the single playback thread; there must be no
        # pre-built queue of pending sends growing in the meantime.
        entered = threading.Event()
        release = threading.Event()

        def slow_send(_data: bytes) -> None:
            entered.set()
            release.wait(2.0)

        client = mock.Mock(spec=udp_client.UDPClient)
        client.send_bytes.side_effect = slow_send
        clock = FakeClock()
        recorder = DatagramRecorder(clock=clock)
        recorder.start()
        for _ in range(50):
            clock.advance(0.001)
            recorder.record(_SIMPLE_MSG_NO_PARAMS)

        player = DatagramPlayer(client)
        player.play(recorder.track, block=False)
        self.assertTrue(entered.wait(1.0))
        # Only the in-flight datagram has been handed to the client.
        self.assertEqual(client.send_bytes.call_count, 1)
        player.cancel()
        release.set()
        player.close()
        self.assertEqual(player.sent_packets, 1)
        self.assertEqual(player.cancelled_packets, 49)


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class TestLoopbackRecordingAndReplay(unittest.TestCase):
    """End-to-end checks over real loopback UDP sockets."""

    def setUp(self) -> None:
        self.port = _free_udp_port()
        self.dispatch = dispatcher.Dispatcher()
        self.received: list[bytes] = []
        self.dispatch.map(
            "/SYNC", lambda address, *args: self.received.append(address)
        )
        self.dispatch.map(
            "/OTHER", lambda address, *args: self.received.append(address)
        )
        self.dispatch.map(
            "/nested", lambda address, *args: self.received.append(address)
        )

    def _send_three_datagrams(self, port: int) -> None:
        sender = udp_client.SimpleUDPClient("127.0.0.1", port)
        sender.send_message("/SYNC", 4)
        time.sleep(0.05)
        sender.send_message("/OTHER", 7)
        time.sleep(0.05)
        nested_builder = osc_message_builder.OscMessageBuilder("/nested")
        nested_builder.add_arg(9)
        bundle = osc_bundle_builder.OscBundleBuilder(987654321)
        bundle.add_content(nested_builder.build())
        sender.send(bundle.build())
        sender.close()

    def test_blocking_server_records_bytes_order_and_keeps_dispatching(self) -> None:
        recorder = DatagramRecorder(max_packets=100)
        server = osc_server.BlockingOSCUDPServer(
            ("127.0.0.1", self.port), self.dispatch, recorder=recorder
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self._send_three_datagrams(self.port)
            deadline = time.monotonic() + 2.0
            while recorder.recorded_count < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)
            recorder.stop()

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.received), 3)
        track = recorder.track
        self.assertEqual(len(track), 3)
        self.assertEqual(track[0].datagram, _SIMPLE_PARAM_INT_MSG)
        self.assertEqual(track[1].datagram, _SECOND_MSG)
        self.assertTrue(track[2].datagram.startswith(b"#bundle\x00"))
        self.assertEqual([p.sequence for p in track], [0, 1, 2])
        self.assertTrue(all(p.relative_time >= 0.0 for p in track))
        self.assertLess(track[0].relative_time, track[1].relative_time)
        self.assertLess(track[1].relative_time, track[2].relative_time)

        # Replay the exact bytes onto a fresh server with an empty dispatcher.
        replay_port = _free_udp_port()
        replay_received: list[bytes] = []
        replay_dispatch = dispatcher.Dispatcher()
        replay_dispatch.map(
            "/SYNC", lambda address, *args: replay_received.append(address)
        )
        replay_dispatch.map(
            "/OTHER", lambda address, *args: replay_received.append(address)
        )
        replay_dispatch.map(
            "/nested", lambda address, *args: replay_received.append(address)
        )
        replay_server = osc_server.BlockingOSCUDPServer(
            ("127.0.0.1", replay_port), replay_dispatch
        )
        replay_thread = threading.Thread(
            target=replay_server.serve_forever, daemon=True
        )
        replay_thread.start()
        client = udp_client.UDPClient("127.0.0.1", replay_port)
        player = DatagramPlayer(client, rate=100.0)
        try:
            sent = player.play(track)
            deadline = time.monotonic() + 2.0
            while len(replay_received) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            player.close()
            client.close()
            replay_server.shutdown()
            replay_server.server_close()
            replay_thread.join(timeout=2.0)

        self.assertEqual(sent, 3)
        self.assertEqual(replay_received, ["/SYNC", "/OTHER", "/nested"])
        self.assertEqual(player.cancelled_packets, 0)

    def test_threading_server_records_in_arrival_order_without_changing_dispatch(
        self,
    ) -> None:
        recorder = DatagramRecorder()
        server = osc_server.ThreadingOSCUDPServer(
            ("127.0.0.1", self.port), self.dispatch, recorder=recorder
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            sender = udp_client.SimpleUDPClient("127.0.0.1", self.port)
            count = 20
            for value in range(count):
                # A deliberately slow handler cannot reorder the recording:
                # verify_request() captures datagrams on the main thread.
                sender.send_message("/SYNC", value)
            sender.close()
            deadline = time.monotonic() + 2.0
            while recorder.recorded_count < count and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)

        self.assertEqual(recorder.recorded_count, count)
        self.assertEqual(len(self.received), count)
        track = recorder.track
        self.assertEqual([p.sequence for p in track], list(range(count)))
        parsed_values = [OscMessage(p.datagram).params[0] for p in track]
        self.assertEqual(parsed_values, list(range(count)))

    def test_bad_datagram_is_not_recorded_but_dispatch_path_unchanged(self) -> None:
        recorder = DatagramRecorder()
        server = osc_server.BlockingOSCUDPServer(
            ("127.0.0.1", self.port), self.dispatch, recorder=recorder
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(_BAD_DATAGRAM, ("127.0.0.1", self.port))
            sock.sendto(_SIMPLE_PARAM_INT_MSG, ("127.0.0.1", self.port))
            sock.close()
            deadline = time.monotonic() + 2.0
            while recorder.recorded_count < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)

        self.assertEqual(recorder.recorded_count, 1)
        self.assertEqual(recorder.track[0].datagram, _SIMPLE_PARAM_INT_MSG)
        # The bad datagram never matched a handler either: pre-existing
        # dispatcher behavior (ParseError swallowed) is preserved.
        self.assertEqual(self.received, ["/SYNC"])

    def test_recorder_failure_never_breaks_dispatch(self) -> None:
        recorder = DatagramRecorder()
        recorder.stop()  # any further record() raises RecorderError
        server = osc_server.BlockingOSCUDPServer(
            ("127.0.0.1", self.port), self.dispatch, recorder=recorder
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            sender = udp_client.SimpleUDPClient("127.0.0.1", self.port)
            sender.send_message("/SYNC", 4)
            sender.close()
            deadline = time.monotonic() + 2.0
            while not self.received and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)

        self.assertEqual(self.received, ["/SYNC"])
        self.assertEqual(recorder.recorded_count, 0)


class TestAsyncioLoopbackRecording(unittest.IsolatedAsyncioTestCase):
    async def test_async_server_records_and_dispatches(self) -> None:
        port = _free_udp_port()
        got: list[str] = []
        dispatch = dispatcher.Dispatcher()
        dispatch.map("/SYNC", lambda address, *args: got.append(address))
        recorder = DatagramRecorder()
        loop = cast(asyncio.BaseEventLoop, asyncio.get_running_loop())
        server = osc_server.AsyncIOOSCUDPServer(
            ("127.0.0.1", port), dispatch, loop, recorder=recorder
        )
        transport, _protocol = await server.create_serve_endpoint()
        try:
            sender = udp_client.SimpleUDPClient("127.0.0.1", port)
            sender.send_message("/SYNC", 4)
            deadline = loop.time() + 2.0
            while recorder.recorded_count < 1 and loop.time() < deadline:
                await asyncio.sleep(0.01)
            sender.close()
        finally:
            transport.close()
            recorder.stop()

        self.assertEqual(got, ["/SYNC"])
        track = recorder.track
        self.assertEqual(len(track), 1)
        self.assertEqual(track[0].datagram, _SIMPLE_PARAM_INT_MSG)

    async def test_async_bad_datagram_skips_recording_only(self) -> None:
        port = _free_udp_port()
        dispatch = dispatcher.Dispatcher()
        recorder = DatagramRecorder()
        loop = cast(asyncio.BaseEventLoop, asyncio.get_running_loop())
        server = osc_server.AsyncIOOSCUDPServer(
            ("127.0.0.1", port), dispatch, loop, recorder=recorder
        )
        transport, _protocol = await server.create_serve_endpoint()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(_BAD_DATAGRAM, ("127.0.0.1", port))
            sock.sendto(_SIMPLE_PARAM_INT_MSG, ("127.0.0.1", port))
            sock.close()
            deadline = loop.time() + 2.0
            while recorder.recorded_count < 1 and loop.time() < deadline:
                await asyncio.sleep(0.01)
        finally:
            transport.close()
            recorder.stop()

        self.assertEqual(recorder.recorded_count, 1)
        self.assertEqual(recorder.track[0].datagram, _SIMPLE_PARAM_INT_MSG)


if __name__ == "__main__":
    unittest.main()
