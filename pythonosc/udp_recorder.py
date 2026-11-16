"""Optional recording and replay of raw UDP datagrams for OSC sessions.

The components in this module never parse or rewrite datagrams: the exact
bytes received on the socket are stored and later re-sent, so OSC messages
and nested bundles (including their raw timetag bytes) round-trip unchanged.

Typical use::

    from pythonosc.osc_server import BlockingOSCUDPServer
    from pythonosc.udp_client import UDPClient
    from pythonosc.udp_recorder import DatagramRecorder, DatagramPlayer

    recorder = DatagramRecorder(max_packets=10000)
    server = BlockingOSCUDPServer((ip, port), dispatcher, recorder=recorder)
    ...  # run the show; the dispatcher keeps working as usual
    recorder.stop()

    player = DatagramPlayer(UDPClient(ip, port), rate=2.0)
    player.play(recorder.track)  # replays the session twice as fast
    player.close()
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from pythonosc.udp_client import UDPClient

logger = logging.getLogger(__name__)


class RecorderError(Exception):
    """Base class for datagram recording failures."""


class ClockRegressionError(RecorderError):
    """Raised when the recording clock goes backwards between two datagrams."""


class CapacityOverflowError(RecorderError):
    """Raised when a full track receives another datagram under RAISE policy."""


class PlaybackError(Exception):
    """Base class for datagram playback failures."""


class InvalidRateError(PlaybackError):
    """Raised when the playback rate is not a finite, positive number."""


class OverflowPolicy(Enum):
    """What happens when a recorder has reached its packet capacity."""

    DROP_OLDEST = "drop_oldest"
    """Discard the oldest recorded datagram to make room for the new one."""

    DROP_NEWEST = "drop_newest"
    """Silently discard the incoming datagram."""

    RAISE = "raise"
    """Raise :class:`CapacityOverflowError` and keep the existing track."""


@dataclass(frozen=True)
class RecordedDatagram:
    """One captured UDP datagram.

    Attributes:
        sequence: Zero-based arrival order across the whole recording session,
            including datagrams that were later dropped by an overflow policy.
        relative_time: Seconds on the monotonic clock between the start of the
            recording session and the arrival of this datagram (never negative).
        datagram: The raw datagram bytes exactly as received.
    """

    sequence: int
    relative_time: float
    datagram: bytes


class RecordingTrack:
    """An immutable, ordered snapshot of recorded datagrams.

    Tracks preserve arrival order and are safe to iterate while the recorder
    that produced them keeps capturing new datagrams.
    """

    def __init__(self, packets: Sequence[RecordedDatagram] = ()) -> None:
        self._packets: tuple[RecordedDatagram, ...] = tuple(packets)

    @property
    def packets(self) -> tuple[RecordedDatagram, ...]:
        """The recorded datagrams in arrival order."""
        return self._packets

    @property
    def is_empty(self) -> bool:
        """True if the track contains no datagrams."""
        return not self._packets

    def __len__(self) -> int:
        return len(self._packets)

    def __iter__(self) -> Iterator[RecordedDatagram]:
        return iter(self._packets)

    def __getitem__(self, index: int) -> RecordedDatagram:
        return self._packets[index]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RecordingTrack) and other._packets == self._packets


class DatagramRecorder:
    """Records raw datagrams with monotonic-clock relative arrival times.

    The recorder is purely in-process: it owns no sockets and no background
    threads, so stopping or dropping it never leaves tasks behind. Memory is
    bounded by ``max_packets`` and the configured :class:`OverflowPolicy`.

    A recorder auto-starts on the first :meth:`record` call; alternatively
    call :meth:`start` explicitly to pin the session's time origin.
    """

    def __init__(
        self,
        max_packets: int = 10000,
        overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize.

        Args:
            max_packets: Maximum number of datagrams kept in the track.
            overflow_policy: How to handle datagrams arriving once the track
                is full.
            clock: Zero-argument callable returning a monotonically increasing
                time in seconds. Defaults to :func:`time.monotonic` and may be
                replaced with an injectable clock in tests.

        Raises:
            ValueError: if ``max_packets`` is not a positive integer.
            TypeError: if an argument has the wrong type.
        """
        if isinstance(max_packets, bool) or not isinstance(max_packets, int):
            raise TypeError("max_packets must be a positive integer")
        if max_packets <= 0:
            raise ValueError("max_packets must be a positive integer")
        if not isinstance(overflow_policy, OverflowPolicy):
            raise TypeError("overflow_policy must be an OverflowPolicy member")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._capacity = max_packets
        self._overflow_policy = overflow_policy
        self._clock = clock
        self._lock = threading.Lock()
        self._packets: list[RecordedDatagram] = []
        self._start_time: Optional[float] = None
        self._last_time: Optional[float] = None
        self._recording = False
        self._stopped = False
        self._dropped_packets = 0
        self._next_sequence = 0

    def start(self) -> None:
        """Begin a recording session and pin the relative-time origin.

        Does nothing when the recorder is already running.

        Raises:
            RecorderError: if the recorder was stopped; call :meth:`reset`.
        """
        with self._lock:
            if self._stopped:
                raise RecorderError(
                    "recorder has been stopped; call reset() before restarting"
                )
            if self._recording:
                return
            self._recording = True
            self._start_time = self._clock()
            self._last_time = self._start_time

    def stop(self) -> None:
        """End the recording session.

        After stopping, :meth:`record` raises until :meth:`reset` is called.
        The captured track is left untouched.
        """
        with self._lock:
            self._recording = False
            self._stopped = True

    def reset(self) -> None:
        """Drop the captured track, counters and timing state and re-arm."""
        with self._lock:
            self._packets.clear()
            self._start_time = None
            self._last_time = None
            self._recording = False
            self._stopped = False
            self._dropped_packets = 0
            self._next_sequence = 0

    def record(self, datagram: bytes) -> Optional[RecordedDatagram]:
        """Record one raw datagram, auto-starting the session on first use.

        Args:
            datagram: Raw datagram bytes exactly as received. The bytes are
                stored untouched and are never parsed or rewritten.

        Returns:
            The stored :class:`RecordedDatagram`, or ``None`` if the datagram
            was dropped under the ``DROP_NEWEST`` overflow policy.

        Raises:
            TypeError: if ``datagram`` is not bytes.
            ValueError: if ``datagram`` is empty.
            ClockRegressionError: if the clock returns a value earlier than the
                previous call.
            CapacityOverflowError: if the track is full under the ``RAISE``
                policy.
            RecorderError: if the recorder has been stopped.
        """
        if not isinstance(datagram, bytes):
            raise TypeError("datagram must be raw bytes")
        if not datagram:
            raise ValueError("cannot record an empty datagram")
        with self._lock:
            if self._stopped:
                raise RecorderError(
                    "recorder has been stopped; call reset() to record again"
                )
            now = self._clock()
            if self._last_time is not None and now < self._last_time:
                raise ClockRegressionError(
                    f"recording clock moved backwards: {now!r} < {self._last_time!r}"
                )
            self._last_time = now
            if not self._recording:
                self._recording = True
                self._start_time = now
            assert self._start_time is not None
            relative_time = now - self._start_time

            if len(self._packets) >= self._capacity:
                if self._overflow_policy is OverflowPolicy.RAISE:
                    raise CapacityOverflowError(
                        f"recording capacity of {self._capacity} packets reached"
                    )
                self._dropped_packets += 1
                if self._overflow_policy is OverflowPolicy.DROP_NEWEST:
                    return None
                self._packets.pop(0)

            packet = RecordedDatagram(
                sequence=self._next_sequence,
                relative_time=relative_time,
                datagram=datagram,
            )
            self._next_sequence += 1
            self._packets.append(packet)
            return packet

    @property
    def track(self) -> RecordingTrack:
        """An immutable snapshot of everything recorded so far."""
        with self._lock:
            return RecordingTrack(self._packets)

    @property
    def is_recording(self) -> bool:
        """Whether the recorder currently accepts datagrams."""
        return self._recording

    @property
    def recorded_count(self) -> int:
        """Number of datagrams currently held in the track."""
        return len(self._packets)

    @property
    def dropped_packets(self) -> int:
        """Number of datagrams discarded by the overflow policy."""
        return self._dropped_packets

    @property
    def capacity(self) -> int:
        """Maximum number of datagrams the track can hold."""
        return self._capacity

    def __enter__(self) -> "DatagramRecorder":
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None
    ) -> None:
        self.stop()


class DatagramPlayer:
    """Replays a :class:`RecordingTrack` through an existing :class:`UDPClient`.

    Playback runs on a single dedicated thread that schedules the next send
    lazily from the recorded relative timestamps: at most one wait is pending
    at any time, there is no queued send list, and a slow receiver therefore
    cannot grow memory without bound. Timing follows
    ``recorded_interval / rate``; the original arrival order is preserved.

    A pending playback can be cancelled before any not-yet-sent datagram is
    transmitted. :meth:`close` cancels playback and joins the thread, so no
    background task survives the player.
    """

    def __init__(
        self,
        client: UDPClient,
        rate: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize.

        Args:
            client: Existing UDP client used to send the recorded bytes.
            rate: Time multiplier applied to recorded intervals; must be a
                finite positive number (2.0 replays twice as fast, 0.5 at
                half speed).
            clock: Zero-argument callable returning a monotonic time in
                seconds; injectable in tests.

        Raises:
            InvalidRateError: if ``rate`` is not finite and positive.
            TypeError: if an argument has the wrong type.
        """
        self._validate_rate(rate)
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._client = client
        self._rate = float(rate)
        self._clock = clock
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._cancel_event = threading.Event()
        self._done_event = threading.Event()
        self._playing = False
        self._sent_packets = 0
        self._cancelled_packets = 0
        self._error: Optional[BaseException] = None

    @staticmethod
    def _validate_rate(rate: object) -> None:
        if isinstance(rate, bool) or not isinstance(rate, (int, float)):
            raise InvalidRateError("playback rate must be a number")
        if not math.isfinite(rate) or rate <= 0:
            raise InvalidRateError(
                f"playback rate must be a finite positive number, got {rate!r}"
            )

    @staticmethod
    def scheduled_delays(track: RecordingTrack, rate: float) -> list[float]:
        """Return the per-packet wait (seconds) the given rate would produce.

        The first delay is the recorded arrival time of the first datagram;
        every later delay is the gap from the previous datagram divided by
        ``rate``. Useful for deterministic timing assertions in tests.
        """
        DatagramPlayer._validate_rate(rate)
        delays: list[float] = []
        previous_time = 0.0
        for packet in track:
            delays.append(max(0.0, (packet.relative_time - previous_time) / rate))
            previous_time = packet.relative_time
        return delays

    def play(
        self, track: RecordingTrack, block: bool = True
    ) -> Optional[int]:
        """Replay a recording track through the UDP client.

        Args:
            track: The recorded datagrams to replay. An empty track completes
                immediately without sending anything.
            block: When True (default), block until playback finishes and
                return the number of datagrams sent. When False, start the
                playback thread and return immediately.

        Returns:
            Number of datagrams sent when blocking, otherwise ``None``.

        Raises:
            PlaybackError: if another playback is already in progress.
            InvalidRateError: cannot occur via the constructor-validated rate;
                re-raised only for misuse of :meth:`scheduled_delays`.
            BaseException: any error raised while sending is re-raised here
                for blocking callers, or from :meth:`wait` otherwise.
        """
        with self._lock:
            if self._playing:
                raise PlaybackError("a playback is already in progress")
            self._cancel_event.clear()
            self._done_event.clear()
            self._error = None
            self._sent_packets = 0
            self._cancelled_packets = 0
            if track.is_empty:
                self._done_event.set()
                return 0
            self._playing = True
            self._thread = threading.Thread(
                target=self._run,
                args=(track,),
                name="osc-datagram-player",
                daemon=True,
            )
            self._thread.start()
        if block:
            return self.wait()
        return None

    def _run(self, track: RecordingTrack) -> None:
        try:
            origin = self._clock()
            for packet in track:
                target_time = origin + packet.relative_time / self._rate
                delay = target_time - self._clock()
                if delay > 0 and self._cancel_event.wait(delay):
                    break
                if self._cancel_event.is_set():
                    break
                self._client.send_bytes(packet.datagram)
                with self._lock:
                    self._sent_packets += 1
        except BaseException as exc:  # surfaced via wait()/play()
            with self._lock:
                self._error = exc
            logger.warning("OSC datagram playback failed: %s", exc)
        finally:
            with self._lock:
                self._playing = False
                if self._cancel_event.is_set():
                    self._cancelled_packets = len(track) - self._sent_packets
            self._done_event.set()

    def wait(self, timeout: Optional[float] = None) -> int:
        """Block until playback finishes.

        Args:
            timeout: Maximum seconds to wait, or None to wait indefinitely.

        Returns:
            Number of datagrams actually sent.

        Raises:
            TimeoutError: if playback is still running after ``timeout``.
            BaseException: re-raises any error encountered on the playback
                thread.
        """
        if not self._done_event.wait(timeout):
            raise TimeoutError("timed out waiting for playback to finish")
        with self._lock:
            if self._error is not None:
                raise self._error
            return self._sent_packets

    def cancel(self) -> None:
        """Cancel playback; datagrams not yet sent will never be transmitted.

        Safe to call when no playback is running, in which case it is a
        no-op until the next :meth:`play` clears the flag.
        """
        self._cancel_event.set()

    def close(self) -> None:
        """Cancel any running playback and join the playback thread.

        Idempotent and safe to call from any thread other than the playback
        thread itself. Leaves no background tasks behind.
        """
        self._cancel_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join()

    @property
    def is_playing(self) -> bool:
        """Whether a playback thread is currently running."""
        return self._playing

    @property
    def rate(self) -> float:
        """The configured playback time multiplier."""
        return self._rate

    @property
    def sent_packets(self) -> int:
        """Number of datagrams sent by the current/last playback."""
        return self._sent_packets

    @property
    def cancelled_packets(self) -> int:
        """Number of not-yet-sent datagrams skipped due to cancellation."""
        return self._cancelled_packets

    def __enter__(self) -> "DatagramPlayer":
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None
    ) -> None:
        self.close()
