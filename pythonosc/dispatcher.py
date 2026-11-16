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
    Dict,
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

#: Maximum accepted length (in characters) for an address template registered
#: with Dispatcher.map_template(). Longer templates are rejected.
MAX_TEMPLATE_LENGTH = 1024

#: Valid names for template parameters (they are passed as keyword arguments).
_TEMPLATE_PARAM_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

#: Characters that start an OSC address pattern wildcard. They are not allowed
#: in the literal parts of an address template.
_OSC_WILDCARD_CHARS = "*?[],"


def _compile_address_template(template: str) -> "re.Pattern[str]":
    """Compiles an address template like '/track/{id}/gain' into a regex.

    Each ``{name}`` segment becomes a named group matching a single non-empty
    path segment. Literal parts are matched verbatim; OSC wildcard characters
    are not allowed in templates (use Dispatcher.map() for those).

    Args:
        template: The address template to compile.

    Returns:
        A compiled regular expression with one named group per template parameter.

    Raises:
        ValueError: If the template is malformed (no leading slash, empty
            segments, unbalanced braces, empty/invalid/duplicate parameter
            names, OSC wildcard characters in literal parts) or exceeds
            MAX_TEMPLATE_LENGTH characters.
    """
    if not isinstance(template, str) or not template.startswith("/"):
        raise ValueError(
            f"Invalid template {template!r}: templates must start with '/'"
        )
    if len(template) > MAX_TEMPLATE_LENGTH:
        raise ValueError(
            f"Invalid template: {len(template)} characters exceeds the maximum "
            f"of {MAX_TEMPLATE_LENGTH}"
        )
    segments = template.split("/")[1:]
    if any(segment == "" for segment in segments):
        raise ValueError(
            f"Invalid template {template!r}: empty path segments are not allowed"
        )

    param_names: List[str] = []
    pattern = "^"
    i = 0
    while i < len(template):
        c = template[i]
        if c == "{":
            end = template.find("}", i + 1)
            if end == -1:
                raise ValueError(
                    f"Invalid template {template!r}: unbalanced '{{'"
                )
            name = template[i + 1 : end]
            if not name:
                raise ValueError(
                    f"Invalid template {template!r}: empty parameter name"
                )
            if not _TEMPLATE_PARAM_NAME_RE.fullmatch(name):
                raise ValueError(
                    f"Invalid template {template!r}: {name!r} is not a valid "
                    "parameter name (must be a Python identifier); note that "
                    "'{a,b}' OSC alternation is only supported by map(), "
                    "not by templates"
                )
            if name in param_names:
                raise ValueError(
                    f"Invalid template {template!r}: duplicate parameter "
                    f"name {name!r}"
                )
            param_names.append(name)
            pattern += f"(?P<{name}>[^/]+)"
            i = end + 1
        elif c == "}":
            raise ValueError(f"Invalid template {template!r}: unbalanced '}}'")
        elif c in _OSC_WILDCARD_CHARS:
            raise ValueError(
                f"Invalid template {template!r}: OSC wildcard character {c!r} "
                "is not allowed in templates; use map() for OSC wildcard "
                "addresses"
            )
        elif c in " #":
            raise ValueError(
                f"Invalid template {template!r}: character {c!r} is not "
                "allowed in OSC addresses"
            )
        else:
            pattern += re.escape(c)
            i += 1
    pattern += "$"
    return re.compile(pattern)


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
        _template_args: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Args:
            _callback Function that is called when handler is invoked
            _args: Message causing invocation
            _needs_reply_address Whether the client's ip address shall be passed as an argument or not
            _template_args Named path segment values extracted when the handler was matched
                through an address template (see Dispatcher.map_template); they are passed to
                the callback as keyword arguments
        """
        self.callback = _callback
        self.args = _args
        self.needs_reply_address = _needs_reply_address
        self.template_args = _template_args

    # needed for test module
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(self, type(other))
            and self.callback == other.callback
            and self.args == other.args
            and self.needs_reply_address == other.needs_reply_address
            and self.template_args == other.template_args
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
        template_args = self.template_args or {}
        if self.needs_reply_address:
            if self.args:
                return self.callback(
                    client_address, message.address, self.args, *message, **template_args
                )
            else:
                return self.callback(
                    client_address, message.address, *message, **template_args
                )
        else:
            if self.args:
                return self.callback(
                    message.address, self.args, *message, **template_args
                )
            else:
                return self.callback(message.address, *message, **template_args)

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
        template_args = self.template_args or {}

        if self.needs_reply_address:
            if self.args:
                if is_async:
                    return await cb(
                        client_address,
                        message.address,
                        self.args,
                        *message,
                        **template_args,
                    )
                else:
                    return cb(
                        client_address,
                        message.address,
                        self.args,
                        *message,
                        **template_args,
                    )
            else:
                if is_async:
                    return await cb(
                        client_address, message.address, *message, **template_args
                    )
                else:
                    return cb(
                        client_address, message.address, *message, **template_args
                    )
        else:
            if self.args:
                if is_async:
                    return await cb(
                        message.address, self.args, *message, **template_args
                    )
                else:
                    return cb(message.address, self.args, *message, **template_args)
            else:
                if is_async:
                    return await cb(message.address, *message, **template_args)
                else:
                    return cb(message.address, *message, **template_args)


class Dispatcher(object):
    """Maps Handlers to OSC addresses and dispatches messages to the handler on matched addresses

    Maps OSC addresses to handler functions and invokes the correct handler when a message comes in.
    """

    def __init__(self, strict_timing: bool = True) -> None:
        """Initialize the dispatcher.

        Args:
            strict_timing: Whether to automatically schedule messages with future timetags.
                If True (default), the dispatcher will wait (using sleep) until the specified
                timetag before invoking handlers.
                If False, messages are dispatched immediately regardless of their timetag.
                Disabling this can prevent memory/thread accumulation issues when receiving
                many future-dated messages.
        """
        self._map: DefaultDict[str, List[Handler]] = collections.defaultdict(list)
        # Address templates registered via map_template(), in registration order.
        # Each entry maps the template string to [compiled regex, handlers].
        self._templates: Dict[str, List[Any]] = {}
        self._default_handler: Optional[Handler] = None
        self._strict_timing = strict_timing

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

    def map_template(
        self,
        template: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> Handler:
        """Map an address template with named path segments to a handler

        A template looks like a regular OSC address except that one or more
        path segments are written as ``{name}``, e.g. ``/track/{id}/gain``.
        Each named segment matches a single non-empty path segment of incoming
        addresses, and its value is passed to the callback as a keyword
        argument (always a string), in addition to the regular OSC arguments:

        .. code-block:: python

            def some_cb(address: str, *osc_args: ArgValue, id: str) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(address: str, fixed_args: List[Any], *osc_args: ArgValue, id: str) -> Union[None, str, Tuple[str, ArgValue]]: ...

        Templates coexist with the exact addresses and OSC wildcard addresses
        registered through map(). When several registrations match the same
        incoming address, all of their handlers are invoked in a stable
        priority order: exact addresses first, then templates (in registration
        order), then OSC wildcard addresses.

        Braces in a template always denote named parameters; the OSC
        ``{a,b}`` alternation wildcard is not supported here (use map() for
        OSC wildcard addresses), and OSC wildcard characters are rejected in
        the literal parts of a template.

        Args:
            template: Address template to be mapped, e.g. '/track/{id}/gain'
            handler: Callback function that will be called as the handler for
                matching addresses
            *args: Fixed arguements that will be passed to the callback function
            needs_reply_address: Whether the IP address from which the message
                originated from shall be passed as an argument to the handler
                callback

        Returns:
            The handler object that will be invoked should an address match
            the given template

        Raises:
            ValueError: If the template is malformed (missing leading slash,
                empty path segments, unbalanced braces, empty, invalid or
                duplicated parameter names, OSC wildcard characters in literal
                parts) or longer than MAX_TEMPLATE_LENGTH characters
        """
        if template not in self._templates:
            self._templates[template] = [_compile_address_template(template), []]
        handlerobj = Handler(handler, list(args), needs_reply_address)
        self._templates[template][1].append(handlerobj)
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

    @overload
    def unmap_template(self, template: str, handler: Handler) -> None:
        """Remove an already mapped handler from an address template

        Args:
            template (str): Template to be unmapped
            handler (Handler): A Handler object as returned from map_template().
        """
        pass

    @overload
    def unmap_template(
        self,
        template: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> None:
        """Remove an already mapped handler from an address template

        Args:
            template: Template to be unmapped
            handler: A function that will be run when an address matches the
                template, with the OscMessage passed as parameter.
            args: Any additional arguments that will be always passed to the
                handlers after the osc messages arguments if any.
            needs_reply_address: True if the handler function needs the
                originating client address passed (as the first argument).
        """
        pass

    def unmap_template(self, template, handler, *args, needs_reply_address=False):
        if template not in self._templates:
            raise ValueError(f"Template '{template}' has no handler mapped to it")
        handlers = self._templates[template][1]
        try:
            if isinstance(handler, Handler):
                handlers.remove(handler)
            else:
                handlers.remove(Handler(handler, list(args), needs_reply_address))
        except ValueError as e:
            if str(e) == "list.remove(x): x not in list":
                raise ValueError(
                    f"Template '{template}' doesn't have handler '{handler}' mapped to it"
                ) from e
        if not handlers:
            del self._templates[template]

    def handlers_for_address(
        self, address_pattern: str
    ) -> Generator[Handler, None, None]:
        """Yields handlers matching an address

        Handlers are yielded in a stable priority order: handlers mapped to
        exact addresses first, then handlers mapped to address templates (in
        registration order, with the extracted named segments bound to each
        yielded handler), then handlers mapped to OSC wildcard addresses.

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
        exact_handlers: List[Handler] = []
        wildcard_handlers: List[Handler] = []
        for addr, handlers in self._map.items():
            if patterncompiled.match(addr) or (
                "*" in addr
                and re.match(addr.replace("*", ".*?") + "$", address_pattern)
            ):
                if any(c in addr for c in "*?[]{"):
                    wildcard_handlers.extend(handlers)
                else:
                    exact_handlers.extend(handlers)

        # Priority 1: exact addresses.
        for handler in exact_handlers:
            matched = True
            yield handler

        # Priority 2: address templates, in registration order. Each yielded
        # handler is bound to the named segments extracted from the address.
        for compiled, handlers in self._templates.values():
            match = compiled.match(address_pattern)
            if match:
                for handler in handlers:
                    matched = True
                    yield Handler(
                        handler.callback,
                        handler.args,
                        handler.needs_reply_address,
                        dict(match.groupdict()),
                    )

        # Priority 3: OSC wildcard addresses.
        for handler in wildcard_handlers:
            matched = True
            yield handler

        if not matched and self._default_handler:
            logging.debug("No handler matched but default handler present, added it.")
            yield self._default_handler

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
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                now = time.time()
                handlers = self.handlers_for_address(timed_msg.message.address)
                if not handlers:
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > now:
                    time.sleep(timed_msg.time - now)
                for handler in handlers:
                    result = handler.invoke(client_address, timed_msg.message)
                    if result is not None:
                        results.append(result)
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
        """

        # Get OSC messages from all bundles or standalone message.
        results = []
        try:
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                now = time.time()
                handlers = self.handlers_for_address(timed_msg.message.address)
                if not handlers:
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > now:
                    await asyncio.sleep(timed_msg.time - now)
                for handler in handlers:
                    result = await handler.async_invoke(
                        client_address, timed_msg.message
                    )
                    if result is not None:
                        results.append(result)
        except osc_packet.ParseError:
            pass
        return results

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
