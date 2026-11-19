"""Maps OSC addresses to handler functions"""

import asyncio
import collections
import inspect
import logging
import re
import threading
import time
from dataclasses import dataclass
from pythonosc import osc_packet
from typing import (
    overload,
    List,
    Union,
    Any,
    AnyStr,
    Dict,
    Generator,
    Tuple,
    Callable,
    Optional,
    DefaultDict,
)
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue
from pythonosc.rate_limit import (
    OverflowPolicy,
    QueuedMessage,
    RateLimitError,
    RateLimitStats,
    RouteRateLimiter,
    SubmitResult,
)


@dataclass(frozen=True)
class _RateConfig:
    """Rate limit configuration stored per mapped address pattern."""

    rate: float
    burst: float
    overflow_policy: OverflowPolicy


class Handler(object):
    """Wrapper for a callback function that will be called when an OSC message is sent to the right address.

    Represents a handler callback function that will be called whenever an OSC message is sent to the address this
    handler is mapped to. It passes the address, the fixed arguments (if any) as well as all osc arguments from the
    message if any were passed.
    """

    def __init__(
        self,
        _callback: Callable,
        _args: Union[Any, List[Any]],
        _needs_reply_address: bool = False,
    ) -> None:
        """
        Args:
            _callback Function that is called when handler is invoked
            _args: Message causing invocation
            _needs_reply_address Whether the client's ip address shall be passed as an argument or not
        """
        self.callback = _callback
        self.args = _args
        self.needs_reply_address = _needs_reply_address

    # needed for test module
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(self, type(other))
            and self.callback == other.callback
            and self.args == other.args
            and self.needs_reply_address == other.needs_reply_address
        )

    def invoke(
        self, client_address: Tuple[str, int], message: OscMessage
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function

        Args:
            client_address: Address match that causes the invocation
            message: Message causing invocation
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        if self.needs_reply_address:
            if self.args:
                return self.callback(
                    client_address, message.address, self.args, *message
                )
            else:
                return self.callback(client_address, message.address, *message)
        else:
            if self.args:
                return self.callback(message.address, self.args, *message)
            else:
                return self.callback(message.address, *message)

    async def async_invoke(
        self, client_address: Tuple[str, int], message: OscMessage
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function (asynchronously)

        Args:
            client_address: Address match that causes the invocation
            message: Message causing invocation
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        cb = self.callback
        is_async = inspect.iscoroutinefunction(cb)

        if self.needs_reply_address:
            if self.args:
                if is_async:
                    return await cb(
                        client_address, message.address, self.args, *message
                    )
                else:
                    return cb(client_address, message.address, self.args, *message)
            else:
                if is_async:
                    return await cb(client_address, message.address, *message)
                else:
                    return cb(client_address, message.address, *message)
        else:
            if self.args:
                if is_async:
                    return await cb(message.address, self.args, *message)
                else:
                    return cb(message.address, self.args, *message)
            else:
                if is_async:
                    return await cb(message.address, *message)
                else:
                    return cb(message.address, *message)


class Dispatcher(object):
    """Maps Handlers to OSC addresses and dispatches messages to the handler on matched addresses

    Maps OSC addresses to handler functions and invokes the correct handler when a message comes in.

    Mapped addresses can optionally be rate limited by passing ``rate`` (and
    optionally ``burst``) to :meth:`map`. Each rate limited address gets its own
    token bucket and its own bounded queue, so a high frequency address can
    never starve slower ones. See :mod:`pythonosc.rate_limit` for the overflow
    policies and statistics.
    """

    def __init__(
        self,
        strict_timing: bool = True,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        async_sleep: Optional[Callable[[float], Any]] = None,
        auto_schedule: bool = True,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            strict_timing: Whether to automatically schedule messages with future timetags.
                If True (default), the dispatcher will wait (using sleep) until the specified
                timetag before invoking handlers.
                If False, messages are dispatched immediately regardless of their timetag.
                Disabling this can prevent memory/thread accumulation issues when receiving
                many future-dated messages.
            clock: Zero-argument callable returning the current time in seconds,
                used both for rate limiting and for resolving bundle timetags.
                Defaults to ``time.time``. Inject a virtual clock in tests.
            sleep: Callable used to block for a number of seconds in the
                synchronous path and in the rate limit scheduler thread.
                Defaults to ``time.sleep``.
            async_sleep: Async callable used to sleep in the asynchronous path
                and its rate limit drain task. Defaults to ``asyncio.sleep``.
            auto_schedule: When True (default), the first rate limited message
                starts a background scheduler thread (synchronous dispatch) or
                asyncio task (asynchronous dispatch) that delivers queued
                messages once tokens refill. Set to False to drive delivery
                manually via :meth:`drain_pending` / :meth:`adrain_pending`,
                which is useful together with a virtual clock in tests.
        """
        self._map: DefaultDict[str, List[Handler]] = collections.defaultdict(list)
        self._default_handler: Optional[Handler] = None
        self._strict_timing = strict_timing
        self._clock = clock if clock is not None else time.time
        self._sleep = sleep if sleep is not None else time.sleep
        self._async_sleep = (
            async_sleep if async_sleep is not None else asyncio.sleep
        )
        self._auto_schedule = auto_schedule
        # address pattern -> rate limit configuration / live limiter
        self._rate_configs: Dict[str, _RateConfig] = {}
        self._limiters: Dict[str, RouteRateLimiter] = {}
        self._lock = threading.RLock()
        # Synchronous background scheduler.
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_wake = threading.Event()
        # Asynchronous background drain task.
        self._async_task: Optional["asyncio.Task[None]"] = None
        self._async_wake: Optional[asyncio.Event] = None
        self._closed = False

    def map(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
        rate: Optional[float] = None,
        burst: Optional[float] = None,
        overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ) -> Handler:
        """Map an address to a handler

        The callback function must have one of the following signatures:

        .. code-block:: python

            def some_cb(address: str, *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(address: str, fixed_args: List[Any], *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], address: str, *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], address: str, fixed_args: List[Any], *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...

        Each variadic OSC argument is a single ArgValue, not a list of all arguments.
        Any fixed arguments passed to map() are collected into the separate fixed_args list.

        The callback function can return None, or a string representing an OSC address to be returned to the client,
        or a tuple that includes the address and ArgValue which will be converted to an OSC message and returned to
        the client.

        Args:
            address: Address to be mapped
            handler: Callback function that will be called as the handler for the given address
            *args: Fixed arguements that will be passed to the callback function
            needs_reply_address: Whether the IP address from which the message originated from shall be passed as
                an argument to the handler callback
            rate: If set, enables per-address rate limiting with this maximum
                sustained dispatch rate in messages per second.
            burst: Maximum burst size in messages. Defaults to ``rate`` (i.e.
                the whole per-second budget may be spent at once). Must be >= 1.
            overflow_policy: What to do when more messages arrive than the
                limiter can queue. :attr:`~pythonosc.rate_limit.OverflowPolicy.DROP_OLDEST`
                (default) discards the oldest queued value so the newest value
                survives; :attr:`~pythonosc.rate_limit.OverflowPolicy.REJECT_NEW`
                drops the incoming message instead.

        Returns:
            The handler object that will be invoked should the given address match

        """
        # TODO: Check the spec:
        # http://opensoundcontrol.org/spec-1_0
        # regarding multiple mappings
        handlerobj = Handler(handler, list(args), needs_reply_address)
        with self._lock:
            self._map[address].append(handlerobj)
            if rate is not None:
                if rate <= 0:
                    raise RateLimitError(
                        f"rate must be > 0, got {rate!r} for {address!r}"
                    )
                effective_burst = rate if burst is None else burst
                if effective_burst <= 0:
                    raise RateLimitError(
                        f"burst must be > 0, got {effective_burst!r} for {address!r}"
                    )
                config = _RateConfig(
                    rate=float(rate),
                    burst=float(effective_burst),
                    overflow_policy=overflow_policy,
                )
                existing = self._rate_configs.get(address)
                if existing is not None and (
                    existing.rate != config.rate
                    or existing.burst != config.burst
                    or existing.overflow_policy != config.overflow_policy
                ):
                    raise RateLimitError(
                        f"Address {address!r} is already rate limited with "
                        f"rate={existing.rate}, burst={existing.burst}, "
                        f"policy={existing.overflow_policy}; re-map without "
                        "rate parameters or call set_rate_limit() to change it"
                    )
                self._rate_configs[address] = config
                limiter = self._limiters.get(address)
                if limiter is not None:
                    limiter.reconfigure(
                        config.rate,
                        config.burst,
                        config.overflow_policy,
                    )
        return handlerobj

    def set_rate_limit(
        self,
        address: str,
        rate: Optional[float],
        burst: Optional[float] = None,
        overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ) -> None:
        """Configure or remove the rate limit of an already mapped address.

        Args:
            address: The exact address pattern previously passed to :meth:`map`.
            rate: Maximum sustained dispatch rate in messages per second, or
                None to remove rate limiting and drop any queued messages.
            burst: Maximum burst size in messages (defaults to ``rate``).
            overflow_policy: Queue overflow strategy.
        """
        with self._lock:
            if rate is None:
                self._rate_configs.pop(address, None)
                limiter = self._limiters.pop(address, None)
                if limiter is not None:
                    limiter.close()
                return
            if rate <= 0:
                raise RateLimitError(
                    f"rate must be > 0, got {rate!r} for {address!r}"
                )
            effective_burst = rate if burst is None else burst
            if effective_burst <= 0:
                raise RateLimitError(
                    f"burst must be > 0, got {effective_burst!r} for {address!r}"
                )
            config = _RateConfig(
                rate=float(rate),
                burst=float(effective_burst),
                overflow_policy=overflow_policy,
            )
            self._rate_configs[address] = config
            limiter = self._limiters.get(address)
            if limiter is None:
                self._limiters[address] = self._make_limiter(address, config)
            else:
                limiter.reconfigure(
                    config.rate, config.burst, config.overflow_policy
                )

    def _make_limiter(
        self, address: str, config: _RateConfig
    ) -> RouteRateLimiter:
        return RouteRateLimiter(
            config.rate,
            config.burst,
            self._clock,
            overflow_policy=config.overflow_policy,
        )

    def _get_limiter(self, address: str) -> Optional[RouteRateLimiter]:
        """Return the live limiter for a mapped pattern, creating it lazily."""
        config = self._rate_configs.get(address)
        if config is None:
            return None
        limiter = self._limiters.get(address)
        if limiter is None:
            limiter = self._make_limiter(address, config)
            self._limiters[address] = limiter
        return limiter

    @overload
    def unmap(self, address: str, handler: Handler) -> None:
        """Remove an already mapped handler from an address

        Args:
            address (str): Address to be unmapped
            handler (Handler): A Handler object as returned from map().
        """
        pass

    @overload
    def unmap(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> None:
        """Remove an already mapped handler from an address

        Args:
            address (str): Address to be unmapped
            handler (str): A function that will be run when the address matches with
                the OscMessage passed as parameter.
            args: Any additional arguments that will be always passed to the
                handlers after the osc messages arguments if any.
            needs_reply_address: True if the handler function needs the
                originating client address passed (as the first argument).
        """
        pass

    def unmap(self, address, handler, *args, needs_reply_address=False):
        try:
            with self._lock:
                if isinstance(handler, Handler):
                    self._map[address].remove(handler)
                else:
                    self._map[address].remove(
                        Handler(handler, list(args), needs_reply_address)
                    )
                if not self._map[address]:
                    # Last handler for this pattern is gone: retire the route
                    # and anything still queued for it.
                    del self._map[address]
                    self._rate_configs.pop(address, None)
                    limiter = self._limiters.pop(address, None)
                    if limiter is not None:
                        limiter.close()
        except ValueError as e:
            if str(e) == "list.remove(x): x not in list":
                raise ValueError(
                    f"Address '{address}' doesn't have handler '{handler}' mapped to it"
                ) from e

    def _routes_for_address(
        self, address_pattern: str
    ) -> Generator[Tuple[str, Handler], None, None]:
        """Yields ``(mapped_pattern, handler)`` tuples matching an address."""
        # Convert OSC Address Pattern to a Python regular expression.
        # Spec: https://opensoundcontrol.stanford.edu/spec-1_0.html#osc-address-patterns

        pattern = "^"
        i = 0
        while i < len(address_pattern):
            c = address_pattern[i]
            if c == "*":
                pattern += "[^/]*"
            elif c == "?":
                pattern += "[^/]"
            elif c == "[":
                pattern += "["
                i += 1
                if i < len(address_pattern) and address_pattern[i] == "!":
                    pattern += "^"
                    i += 1
                while i < len(address_pattern) and address_pattern[i] != "]":
                    if address_pattern[i] in r"\^$.|()+*?":
                        pattern += "\\"
                    pattern += address_pattern[i]
                    i += 1
                pattern += "]"
            elif c == "{":
                pattern += "("
                i += 1
                while i < len(address_pattern) and address_pattern[i] != "}":
                    char = address_pattern[i]
                    if char == ",":
                        pattern += "|"
                    elif char in r"\^$.|()[]+*?":
                        pattern += "\\" + char
                    else:
                        pattern += char
                    i += 1
                pattern += ")"
            elif c in r"\^$.|()[]+?":
                pattern += "\\" + c
            else:
                pattern += c
            i += 1
        pattern += "$"

        try:
            patterncompiled = re.compile(pattern)
        except re.error:
            # If the pattern is invalid, it won't match anything.
            return

        matched = False
        for addr, handlers in self._map.items():
            if patterncompiled.match(addr) or (
                "*" in addr
                and re.match(addr.replace("*", ".*?") + "$", address_pattern)
            ):
                for handler in handlers:
                    yield addr, handler
                matched = True

        if not matched and self._default_handler:
            logging.debug("No handler matched but default handler present, added it.")
            # The default handler has no pattern of its own and is never
            # rate limited.
            yield "", self._default_handler

    def handlers_for_address(
        self, address_pattern: str
    ) -> Generator[Handler, None, None]:
        """Yields handlers matching an address

        Args:
            address_pattern: Address to match

        Returns:
            Generator yielding Handlers matching address_pattern
        """
        for _pattern, handler in self._routes_for_address(address_pattern):
            yield handler

    def rate_limit_stats(self, address: Optional[str] = None) -> Any:
        """Return rate limiting statistics.

        Args:
            address: Mapped pattern to query. If None, returns a dict of
                :class:`~pythonosc.rate_limit.RateLimitStats` keyed by pattern
                for every rate limited route.

        Raises:
            KeyError: if ``address`` is not a rate limited mapped pattern.
        """
        with self._lock:
            if address is not None:
                limiter = self._limiters.get(address)
                if limiter is None:
                    config = self._rate_configs.get(address)
                    if config is None:
                        raise KeyError(
                            f"No rate limit configured for address {address!r}"
                        )
                    return RateLimitStats()
                return limiter.stats
            return {
                pattern: limiter.stats
                for pattern, limiter in self._limiters.items()
            }

    # ------------------------------------------------------------------
    # Synchronous dispatch
    # ------------------------------------------------------------------

    def _wait_until(self, timetag: float) -> None:
        """Sleep until ``timetag`` when strict timing requires it."""
        if not self._strict_timing:
            return
        now = self._clock()
        if timetag > now:
            self._sleep(timetag - now)

    def _invoke_queued_sync(self, item: QueuedMessage) -> None:
        """Deliver one released queued message through its handlers."""
        self._wait_until(item.time)
        for handler in item.handlers:
            handler.invoke(item.client_address, item.message)

    def drain_pending(self) -> int:
        """Deliver every queued message whose rate limit token is available.

        This is called automatically while packets are processed and by the
        background scheduler (if enabled), but can also be driven manually,
        e.g. with a virtual clock in tests.

        Returns:
            Number of queued messages delivered.
        """
        delivered = 0
        with self._lock:
            limiters = list(self._limiters.values())
        for limiter in limiters:
            for item in limiter.pump_due(self._strict_timing):
                self._invoke_queued_sync(item)
                delivered += 1
        return delivered

    def _scheduler_loop(self) -> None:
        """Background thread: release queued messages as tokens refill."""
        while True:
            with self._lock:
                if self._closed:
                    return
                wakeups = [
                    wake
                    for limiter in self._limiters.values()
                    if (wake := limiter.next_wakeup(self._strict_timing))
                    is not None
                ]
            self.drain_pending()
            if not wakeups:
                # Park until a new message queues something or shutdown.
                self._scheduler_wake.wait()
                self._scheduler_wake.clear()
                continue
            delay = max(0.0, min(wakeups) - self._clock())
            if self._scheduler_wake.wait(delay):
                self._scheduler_wake.clear()

    def _ensure_scheduler(self) -> None:
        if not self._auto_schedule or self._closed:
            return
        if (
            self._scheduler_thread is None
            or not self._scheduler_thread.is_alive()
        ):
            self._scheduler_wake.clear()
            thread = threading.Thread(
                target=self._scheduler_loop,
                name="pythonosc-rate-limit-scheduler",
                daemon=True,
            )
            self._scheduler_thread = thread
            thread.start()

    def call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """Invoke handlers for all messages in OSC packet

        The incoming OSC Packet is decoded and the handlers for each included message is found and invoked.

        Args:
            data: Data of packet
            client_address: Address of client this packet originated from
        Returns: A list of strings or tuples to be converted to OSC messages and returned to the client
        """
        results = list()
        # Get OSC messages from all bundles or standalone message.
        try:
            packet = osc_packet.OscPacket(data, now=self._clock())
            for timed_msg in packet.messages:
                routes = list(
                    self._routes_for_address(timed_msg.message.address)
                )
                if not routes:
                    continue
                # Group handlers by the mapped pattern they belong to so each
                # rate limited route can make its own admission decision.
                handlers_by_pattern: DefaultDict[
                    str, List[Handler]
                ] = collections.defaultdict(list)
                for pattern, handler in routes:
                    handlers_by_pattern[pattern].append(handler)

                limited = [
                    pattern
                    for pattern in handlers_by_pattern
                    if pattern and pattern in self._rate_configs
                ]
                if not limited:
                    # Legacy behavior: timetag wait, then all handlers inline.
                    self._wait_until(timed_msg.time)
                    for handler in (
                        h for hs in handlers_by_pattern.values() for h in hs
                    ):
                        result = handler.invoke(
                            client_address, timed_msg.message
                        )
                        if result is not None:
                            results.append(result)
                    continue

                # A message matched by several limited routes must be admitted
                # by all of them; a single rejection drops it everywhere.
                with self._lock:
                    decisions = []
                    rejected = False
                    for pattern in limited:
                        limiter = self._get_limiter(pattern)
                        assert limiter is not None
                        decision = limiter.submit(
                            client_address,
                            timed_msg.message,
                            timed_msg.time,
                            handlers_by_pattern[pattern],
                            honor_timetag=self._strict_timing,
                        )
                        decisions.append((pattern, limiter, decision))
                        if decision in (
                            SubmitResult.REJECTED,
                        ):
                            rejected = True
                    if rejected:
                        # Roll back any queue/delivery tokens already consumed
                        # by the routes that accepted this message.
                        for pattern, limiter, decision in decisions:
                            if decision is not SubmitResult.REJECTED:
                                limiter.rollback_submission(
                                    timed_msg.message, decision
                                )
                        continue
                    self._ensure_scheduler()
                    if any(
                        decision is SubmitResult.QUEUED
                        for _p, _l, decision in decisions
                    ):
                        self._scheduler_wake.set()
                        if self._async_wake is not None:
                            self._async_wake.set()

                for _pattern, _limiter, decision in decisions:
                    if decision is SubmitResult.DELIVERED:
                        self._wait_until(timed_msg.time)
                        for handler in handlers_by_pattern[_pattern]:
                            result = handler.invoke(
                                client_address, timed_msg.message
                            )
                            if result is not None:
                                results.append(result)
                # Any QUEUED messages are released later by drain_pending() /
                # the scheduler thread; their handler results cannot be sent
                # back as packet responses anymore.
                self.drain_pending()
        except osc_packet.ParseError:
            pass
        return results

    # ------------------------------------------------------------------
    # Asynchronous dispatch
    # ------------------------------------------------------------------

    async def _async_wait_until(self, timetag: float) -> None:
        if not self._strict_timing:
            return
        now = self._clock()
        if timetag > now:
            await self._async_sleep(timetag - now)

    async def _invoke_queued_async(self, item: QueuedMessage) -> None:
        await self._async_wait_until(item.time)
        for handler in item.handlers:
            await handler.async_invoke(item.client_address, item.message)

    async def adrain_pending(self) -> int:
        """Async counterpart of :meth:`drain_pending`."""
        delivered = 0
        with self._lock:
            limiters = list(self._limiters.values())
        for limiter in limiters:
            for item in limiter.pump_due(self._strict_timing):
                await self._invoke_queued_async(item)
                delivered += 1
        return delivered

    async def _async_scheduler_loop(self) -> None:
        """Background asyncio task: release queued messages as tokens refill."""
        assert self._async_wake is not None
        while True:
            with self._lock:
                if self._closed:
                    return
                wakeups = [
                    wake
                    for limiter in self._limiters.values()
                    if (wake := limiter.next_wakeup(self._strict_timing))
                    is not None
                ]
            await self.adrain_pending()
            if not wakeups:
                await self._async_wake.wait()
                self._async_wake.clear()
                continue
            delay = max(0.0, min(wakeups) - self._clock())
            try:
                await asyncio.wait_for(self._async_wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            else:
                self._async_wake.clear()

    def _ensure_async_scheduler(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        if not self._auto_schedule or self._closed:
            return
        if self._async_task is None or self._async_task.done():
            if self._async_wake is None:
                self._async_wake = asyncio.Event()
            else:
                self._async_wake.clear()
            self._async_task = loop.create_task(self._async_scheduler_loop())

    async def async_call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """
        This function calls the handlers registered to the dispatcher for
        every message it found in the packet.
        The process/thread granularity is thus the OSC packet, not the handler.

        If parameters were registered with the dispatcher, then the handlers are
        called this way:
          handler('/address that triggered the message',
                  registered_param_list, osc_msg_arg1, osc_msg_arg2, ...)
        if no parameters were registered, then it is just called like this:
          handler('/address that triggered the message',
                  osc_msg_arg1, osc_msg_arg2, osc_msg_param3, ...)
        """

        # Get OSC messages from all bundles or standalone message.
        results = []
        try:
            packet = osc_packet.OscPacket(data, now=self._clock())
            for timed_msg in packet.messages:
                routes = list(
                    self._routes_for_address(timed_msg.message.address)
                )
                if not routes:
                    continue
                handlers_by_pattern: DefaultDict[
                    str, List[Handler]
                ] = collections.defaultdict(list)
                for pattern, handler in routes:
                    handlers_by_pattern[pattern].append(handler)

                limited = [
                    pattern
                    for pattern in handlers_by_pattern
                    if pattern and pattern in self._rate_configs
                ]
                if not limited:
                    await self._async_wait_until(timed_msg.time)
                    for handler in (
                        h for hs in handlers_by_pattern.values() for h in hs
                    ):
                        result = await handler.async_invoke(
                            client_address, timed_msg.message
                        )
                        if result is not None:
                            results.append(result)
                    continue

                with self._lock:
                    decisions = []
                    rejected = False
                    for pattern in limited:
                        limiter = self._get_limiter(pattern)
                        assert limiter is not None
                        decision = limiter.submit(
                            client_address,
                            timed_msg.message,
                            timed_msg.time,
                            handlers_by_pattern[pattern],
                            honor_timetag=self._strict_timing,
                        )
                        decisions.append((pattern, limiter, decision))
                        if decision is SubmitResult.REJECTED:
                            rejected = True
                    if rejected:
                        for pattern, limiter, decision in decisions:
                            if decision is not SubmitResult.REJECTED:
                                limiter.rollback_submission(
                                    timed_msg.message, decision
                                )
                        continue
                    self._ensure_async_scheduler(asyncio.get_running_loop())
                    if any(
                        decision is SubmitResult.QUEUED
                        for _p, _l, decision in decisions
                    ):
                        self._scheduler_wake.set()
                        if self._async_wake is not None:
                            self._async_wake.set()

                for pattern, _limiter, decision in decisions:
                    if decision is SubmitResult.DELIVERED:
                        await self._async_wait_until(timed_msg.time)
                        for handler in handlers_by_pattern[pattern]:
                            result = await handler.async_invoke(
                                client_address, timed_msg.message
                            )
                            if result is not None:
                                results.append(result)
                await self.adrain_pending()
        except osc_packet.ParseError:
            pass
        return results

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self, flush: bool = False) -> int:
        """Stop background scheduling and close every rate limiter.

        Idempotent. Rate limiting is disabled afterwards: messages on
        unlimited routes are still dispatched, while rate limited routes
        reject new messages.

        Args:
            flush: If True, deliver all currently queued messages first
                (ignoring remaining tokens, but still honoring timetags).
                If False (default), queued messages are dropped and counted in
                the per-route statistics.

        Returns:
            Number of messages dropped while closing (0 when flush=True).
        """
        with self._lock:
            if self._closed:
                return 0
            self._closed = True
            self._scheduler_wake.set()
        if self._scheduler_thread is not None:
            self._scheduler_thread.join()
            self._scheduler_thread = None
        if flush:
            self._flush_queued_sync()
        dropped = 0
        with self._lock:
            for limiter in self._limiters.values():
                dropped += limiter.close()
        return dropped

    def _flush_queued_sync(self) -> None:
        """Deliver everything still queued, in timetag/FIFO order."""
        with self._lock:
            pending = [
                item
                for limiter in self._limiters.values()
                for item in limiter.drain_all()
            ]
        pending.sort(key=lambda item: item.time)
        for item in pending:
            self._invoke_queued_sync(item)

    async def ashutdown(self, flush: bool = False) -> int:
        """Async counterpart of :meth:`shutdown`."""
        with self._lock:
            if self._closed:
                return 0
            self._closed = True
            self._scheduler_wake.set()
            if self._async_wake is not None:
                self._async_wake.set()
        if self._scheduler_thread is not None:
            self._scheduler_thread.join()
            self._scheduler_thread = None
        if self._async_task is not None:
            try:
                await self._async_task
            finally:
                self._async_task = None
        if flush:
            await self._aflush_queued()
        dropped = 0
        with self._lock:
            for limiter in self._limiters.values():
                dropped += limiter.close()
        return dropped

    async def _aflush_queued(self) -> None:
        with self._lock:
            pending = [
                item
                for limiter in self._limiters.values()
                for item in limiter.drain_all()
            ]
        pending.sort(key=lambda item: item.time)
        for item in pending:
            await self._invoke_queued_async(item)

    def set_default_handler(
        self, handler: Callable, needs_reply_address: bool = False
    ) -> None:
        """Sets the default handler

        The default handler is invoked every time no other handler is mapped to an address.

        Args:
            handler: Callback function to handle unmapped requests
            needs_reply_address: Whether the callback shall be passed the client address
        """
        self._default_handler = (
            None if (handler is None) else Handler(handler, [], needs_reply_address)
        )
