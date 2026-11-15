"""Maps OSC addresses to handler functions"""

import asyncio
import collections
import inspect
import logging
import re
import time
from pythonosc import osc_packet
from typing import (
    overload,
    List,
    Union,
    Any,
    AnyStr,
    Generator,
    Tuple,
    Callable,
    Optional,
    DefaultDict,
)
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue
from pythonosc.scheduling import EventScheduler


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
    """

    def __init__(
        self,
        strict_timing: bool = True,
        scheduler: Optional[EventScheduler] = None,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            strict_timing: Whether to automatically schedule messages with future timetags.
                If True (default) and no ``scheduler`` is given, the dispatcher will wait
                (using sleep) until the specified timetag before invoking handlers.
                If False, messages are dispatched immediately regardless of their timetag.
                Disabling this can prevent memory/thread accumulation issues when receiving
                many future-dated messages.
            scheduler: Optional bounded scheduler
                (:class:`~pythonosc.scheduling.ThreadEventScheduler` for the synchronous
                servers, :class:`~pythonosc.scheduling.AsyncIOEventScheduler` for the
                asyncio ones).  When provided, messages with future timetags are enqueued
                and fired later without blocking the receive path, with a bounded queue,
                configurable overflow/close policies, observability and cancellation.
                It takes precedence over ``strict_timing`` for future timetags; stale
                messages and IMMEDIATELY bundles still fire at once.
        """
        self._map: DefaultDict[str, List[Handler]] = collections.defaultdict(list)
        self._default_handler: Optional[Handler] = None
        self._strict_timing = strict_timing
        self._scheduler = scheduler

    @property
    def scheduler(self) -> Optional[EventScheduler]:
        """The bounded scheduler future timetags are handed to, if any."""
        return self._scheduler

    def cancel_scheduled(self) -> int:
        """Cancel every event still pending in the scheduler.

        Returns:
            The number of events cancelled.  Returns 0 when no scheduler is
            configured.
        """
        if self._scheduler is None:
            return 0
        return self._scheduler.cancel_all()

    def map(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
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

        Returns:
            The handler object that will be invoked should the given address match

        """
        # TODO: Check the spec:
        # http://opensoundcontrol.org/spec-1_0
        # regarding multiple mappings
        handlerobj = Handler(handler, list(args), needs_reply_address)
        self._map[address].append(handlerobj)
        return handlerobj

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
            address: Address to be unmapped
            handler: A function that will be run when the address matches with
                the OscMessage passed as parameter.
            args: Any additional arguments that will be always passed to the
                handlers after the osc messages arguments if any.
            needs_reply_address: True if the handler function needs the
                originating client address passed (as the first argument).
        """
        pass

    def unmap(self, address, handler, *args, needs_reply_address=False):
        try:
            if isinstance(handler, Handler):
                self._map[address].remove(handler)
            else:
                self._map[address].remove(
                    Handler(handler, list(args), needs_reply_address)
                )
        except ValueError as e:
            if str(e) == "list.remove(x): x not in list":
                raise ValueError(
                    f"Address '{address}' doesn't have handler '{handler}' mapped to it"
                ) from e

    def handlers_for_address(
        self, address_pattern: str
    ) -> Generator[Handler, None, None]:
        """Yields handlers matching an address

        Args:
            address_pattern: Address to match

        Returns:
            Generator yielding Handlers matching address_pattern
        """
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
                yield from handlers
                matched = True

        if not matched and self._default_handler:
            logging.debug("No handler matched but default handler present, added it.")
            yield self._default_handler

    def _invoke_handlers(
        self,
        handlers: List[Handler],
        client_address: Tuple[str, int],
        message: OscMessage,
        results: List,
    ) -> None:
        """Invoke matching handlers synchronously, collecting reply results.

        Exceptions raised by a handler propagate to the caller, exactly as in
        the immediate/strict-timing paths; isolation for deferred callbacks is
        provided by the scheduler itself.
        """
        for handler in handlers:
            result = handler.invoke(client_address, message)
            if result is not None:
                results.append(result)

    async def _async_invoke_handlers(
        self,
        handlers: List[Handler],
        client_address: Tuple[str, int],
        message: OscMessage,
        results: List,
    ) -> None:
        """Invoke matching handlers, awaiting coroutine callbacks."""
        for handler in handlers:
            result = await handler.async_invoke(client_address, message)
            if result is not None:
                results.append(result)

    def call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """Invoke handlers for all messages in OSC packet

        The incoming OSC Packet is decoded and the handlers for each included message is found and invoked.

        Args:
            data: Data of packet
            client_address: Address of client this packet originated from
        Returns: A list of strings or tuples to be converted to OSC messages and returned to the client.
            Replies can only be produced for messages dispatched immediately; messages deferred to a
            scheduler are fire-and-forget.
        """
        results: List = list()
        # Get OSC messages from all bundles or standalone message.
        try:
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                handlers = list(self.handlers_for_address(timed_msg.message.address))
                if not handlers:
                    continue
                if self._defer_if_future(timed_msg, handlers, client_address):
                    # Bounded, non-blocking mode: accepted now, fires later.
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > time.time():
                    time.sleep(timed_msg.time - time.time())
                self._invoke_handlers(
                    handlers, client_address, timed_msg.message, results
                )
        except osc_packet.ParseError:
            pass
        return results

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

        Replies can only be produced for messages dispatched immediately; messages
        deferred to a scheduler are fire-and-forget.
        """

        # Get OSC messages from all bundles or standalone message.
        results: List = []
        try:
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                handlers = list(self.handlers_for_address(timed_msg.message.address))
                if not handlers:
                    continue
                if await self._adefer_if_future(timed_msg, handlers, client_address):
                    # Bounded, non-blocking mode: accepted now, fires later.
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > time.time():
                    await asyncio.sleep(timed_msg.time - time.time())
                await self._async_invoke_handlers(
                    handlers, client_address, timed_msg.message, results
                )
        except osc_packet.ParseError:
            pass
        return results

    def _defer_if_future(
        self,
        timed_msg: osc_packet.TimedMessage,
        handlers: List[Handler],
        client_address: Tuple[str, int],
    ) -> bool:
        """Hand a future-timetag message to the synchronous scheduler, if any.

        Returns:
            True if the message was deferred and must not be dispatched now.
        """
        if self._scheduler is None or timed_msg.raw_time is None:
            return False
        if timed_msg.raw_time <= self._scheduler.now():
            return False
        message = timed_msg.message
        self._scheduler.schedule(
            timed_msg.raw_time,
            lambda: self._invoke_handlers(handlers, client_address, message, []),
        )
        return True

    async def _adefer_if_future(
        self,
        timed_msg: osc_packet.TimedMessage,
        handlers: List[Handler],
        client_address: Tuple[str, int],
    ) -> bool:
        """Hand a future-timetag message to the asyncio scheduler, if any.

        Returns:
            True if the message was deferred and must not be dispatched now.
        """
        if self._scheduler is None or timed_msg.raw_time is None:
            return False
        if timed_msg.raw_time <= self._scheduler.now():
            return False
        message = timed_msg.message
        self._scheduler.schedule(
            timed_msg.raw_time,
            lambda: self._schedule_async(handlers, client_address, message),
        )
        return True

    def _schedule_async(
        self,
        handlers: List[Handler],
        client_address: Tuple[str, int],
        message: OscMessage,
    ) -> Any:
        """Entry point used by an AsyncIOEventScheduler for deferred messages."""
        return self._async_invoke_handlers(handlers, client_address, message, [])

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
