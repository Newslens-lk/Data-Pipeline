import asyncio
import collections.abc
import datetime
import enum
import modal._supports_logs
import modal.types
import modal_proto.api_pb2
import typing
import typing_extensions

LogSource = typing.Literal["stdout", "stderr", "system"]

class _Deadline:
    """_Deadline(value: 'float | None' = None)"""

    value: typing.Optional[float]

    def reset(self, timeout: typing.Optional[float] = None) -> None: ...
    def __init__(self, value: typing.Optional[float] = None) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
        ...

    def __repr__(self):
        """Return repr(self)."""
        ...

    def __eq__(self, other):
        """Return self==value."""
        ...

class _StreamStopReason(enum.Enum):
    IDLE_TIMEOUT = "idle_timeout"
    STOP_STREAM = "stop_stream"

def _normalize_utc_datetime(value: datetime.datetime, name: str) -> datetime.datetime: ...
def _entry_source(file_descriptor: int) -> typing.Literal["stdout", "stderr", "system"]: ...
def _entry_timestamp(item: modal_proto.api_pb2.TaskLogs) -> datetime.datetime: ...
def _resolve_source(source: typing.Optional[typing.Literal["stdout", "stderr", "system"]]) -> int: ...
def _entry_context_ids(
    object_id: str, item: modal_proto.api_pb2.TaskLogs, batch: modal_proto.api_pb2.TaskLogsBatch
) -> list[str]: ...
def _entry_from_item(
    object_id: str, item: modal_proto.api_pb2.TaskLogs, batch: modal_proto.api_pb2.TaskLogsBatch
) -> modal.types.LogEntry: ...

class _LogsManager:
    """mdmd:namespace"""
    def __init__(
        self,
        source: modal._supports_logs._SupportsLogs,
        stop_stream: typing.Optional[collections.abc.Callable[[], collections.abc.Awaitable[bool]]] = None,
    ):
        """mdmd:hidden"""
        ...

    async def _params(self) -> modal._supports_logs._LogQueryData: ...
    async def _watch_stream_stop(self, deadline: _Deadline) -> typing.Optional[_StreamStopReason]: ...
    def fetch(
        self,
        *,
        since: datetime.datetime,
        until: typing.Optional[datetime.datetime] = None,
        source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
        search_text: str = "",
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch all associated logs corresponding to the date range and filters."""
        ...

    def tail(
        self, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent logs."""
        ...

    def _create_log_stream(
        self, params: modal._supports_logs._LogQueryData, last_entry_id: str, timeout: float
    ) -> collections.abc.AsyncGenerator[modal_proto.api_pb2.TaskLogsBatch, None]:
        """Helper for creating the log stream RPC."""
        ...

    def _stream_entries(self, batch: modal_proto.api_pb2.TaskLogsBatch, source_object_id: str): ...
    @staticmethod
    def _advance_batch(batch: modal_proto.api_pb2.TaskLogsBatch, last_entry_id: str) -> tuple[str, bool]: ...
    @staticmethod
    def _is_transient_stream_error(exc: Exception) -> bool: ...
    def _drain_stream(
        self, params: modal._supports_logs._LogQueryData, last_entry_id: str, timeout: float
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Do a final bounded drain in the logs to catch any remaining logs after the stop condition is met."""
        ...

    def stream(
        self, timeout: typing.Optional[float] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Stream new logs until no logs arrive within the timeout."""
        ...

    @staticmethod
    async def _suppress_cancelled(task: asyncio.Task) -> None: ...

class _FunctionLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    def fetch(
        self,
        *,
        since: datetime.datetime,
        until: typing.Optional[datetime.datetime] = None,
        source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
        search_text: str = "",
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch Function logs corresponding to the date range and filters.

        Args:
            since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                which is interpreted as local time.
            until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                as local time.
            source: Filter by source: 'stdout', 'stderr', or 'system'.
            search_text: Filter by search text.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")

            for entry in function.logs.fetch(
                since=datetime.now() - timedelta(hours=4),
                source="stdout",
            ):
                print(entry.message, end="")
            ```
        """
        ...

    def tail(
        self, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent Function logs.

        Args:
            entries: The number of log entries to return.
            source: Filter by source: 'stdout', 'stderr', or 'system'.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")

            for entry in function.logs.tail(20):
                print(entry.message, end="")
            ```
        """
        ...

    def stream(
        self, timeout: typing.Optional[float] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Stream new Function logs until the timeout is reached.

        Args:
            timeout: Number of seconds to wait between log entries before terminating the stream.
                By default, this will block until it is interrupted.

        Yields:
            `LogEntry` objects as they arrive.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")

            for entry in function.logs.stream(timeout=60):
                print(entry.message, end="")
            ```
        """
        ...

class FunctionLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    class __fetch_spec(typing_extensions.Protocol):
        def __call__(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch Function logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.fetch(
                    since=datetime.now() - timedelta(hours=4),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch Function logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.fetch(
                    since=datetime.now() - timedelta(hours=4),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

    fetch: __fetch_spec

    class __tail_spec(typing_extensions.Protocol):
        def __call__(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch the most recent Function logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch the most recent Function logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

    tail: __tail_spec

    class __stream_spec(typing_extensions.Protocol):
        def __call__(
            self, /, timeout: typing.Optional[float] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Stream new Function logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, timeout: typing.Optional[float] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Stream new Function logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")

                for entry in function.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

    stream: __stream_spec

class _ServerLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    def fetch(
        self,
        *,
        since: datetime.datetime,
        until: typing.Optional[datetime.datetime] = None,
        source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
        search_text: str = "",
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch Server logs corresponding to the date range and filters.

        Args:
            since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                which is interpreted as local time.
            until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                as local time.
            source: Filter by source: 'stdout', 'stderr', or 'system'.
            search_text: Filter by search text.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            server = modal.Server.from_name("my-app", "web")

            for entry in server.logs.fetch(
                since=datetime.now() - timedelta(minutes=25),
                source="stdout",
            ):
                print(entry.message, end="")
            ```
        """
        ...

    def tail(
        self, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent Server logs.

        Args:
            entries: The number of log entries to return.
            source: Filter by source: 'stdout', 'stderr', or 'system'.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            server = modal.Server.from_name("my-app", "web")

            for entry in server.logs.tail(20):
                print(entry.message, end="")
            ```
        """
        ...

    def stream(
        self, timeout: typing.Optional[float] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Stream new Server logs until the timeout is reached.

        Args:
            timeout: Number of seconds to wait between log entries before terminating the stream.
                By default, this will block until it is interrupted.

        Yields:
            `LogEntry` objects as they arrive.

        Examples:

            ```python notest
            server = modal.Server.from_name("my-app", "web")

            for entry in server.logs.stream(timeout=60):
                print(entry.message, end="")
            ```
        """
        ...

class ServerLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    class __fetch_spec(typing_extensions.Protocol):
        def __call__(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch Server logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.fetch(
                    since=datetime.now() - timedelta(minutes=25),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch Server logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.fetch(
                    since=datetime.now() - timedelta(minutes=25),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

    fetch: __fetch_spec

    class __tail_spec(typing_extensions.Protocol):
        def __call__(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch the most recent Server logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch the most recent Server logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

    tail: __tail_spec

    class __stream_spec(typing_extensions.Protocol):
        def __call__(
            self, /, timeout: typing.Optional[float] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Stream new Server logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, timeout: typing.Optional[float] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Stream new Server logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                server = modal.Server.from_name("my-app", "web")

                for entry in server.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

    stream: __stream_spec

class _FunctionCallLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    async def _get_function_call_info(self) -> modal_proto.api_pb2.FunctionCallInfo: ...
    async def _determine_function_call_stop(self) -> bool: ...
    def stream(
        self, timeout: typing.Optional[float] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Stream new FunctionCall logs until the timeout is reached.
        The timeout specifies the number of seconds to wait between log entries before terminating the stream.
        This method will stop when the FunctionCall is observed to have completed,
        or when the timeout is reached. The completion check is best-effort; if completion
        cannot be determined, the stream will continue until the timeout is reached.

        Args:
            timeout: Number of seconds to wait between log entries before terminating the stream.
               By default, this will block until it is interrupted.

        Yields:
            `LogEntry` objects as they arrive.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")
            call = function.spawn()

            for entry in call.logs.stream():
                print(entry.message, end="")
            ```
        """
        ...

    def tail(
        self, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent FunctionCall logs.

        Args:
            entries: The number of log entries to return.
            source: Filter by source: 'stdout', 'stderr', or 'system'.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")
            call = function.spawn()

            for entry in call.logs.tail(entries=10):
                print(entry.timestamp, entry.message, end="")
            ```
        """
        ...

    def fetch(
        self,
        *,
        since: typing.Optional[datetime.datetime] = None,
        until: typing.Optional[datetime.datetime] = None,
        source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
        search_text: str = "",
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch all associated logs corresponding to the date range and filters.

        Args:
            since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                which is interpreted as local time.
                By default, this will fetch logs from the start of the function call.
            until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                as local time.
            source: Filter by source: 'stdout', 'stderr', or 'system'.
            search_text: Filter by search text.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            function = modal.Function.from_name("my-app", "train")
            call = function.spawn()

            for entry in call.logs.fetch():
                print(entry.timestamp, entry.message, end="")
            ```
        """
        ...

class FunctionCallLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    class ___get_function_call_info_spec(typing_extensions.Protocol):
        def __call__(self, /) -> modal_proto.api_pb2.FunctionCallInfo: ...
        async def aio(self, /) -> modal_proto.api_pb2.FunctionCallInfo: ...

    _get_function_call_info: ___get_function_call_info_spec

    class ___determine_function_call_stop_spec(typing_extensions.Protocol):
        def __call__(self, /) -> bool: ...
        async def aio(self, /) -> bool: ...

    _determine_function_call_stop: ___determine_function_call_stop_spec

    class __stream_spec(typing_extensions.Protocol):
        def __call__(
            self, /, timeout: typing.Optional[float] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Stream new FunctionCall logs until the timeout is reached.
            The timeout specifies the number of seconds to wait between log entries before terminating the stream.
            This method will stop when the FunctionCall is observed to have completed,
            or when the timeout is reached. The completion check is best-effort; if completion
            cannot be determined, the stream will continue until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                   By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.stream():
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, timeout: typing.Optional[float] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Stream new FunctionCall logs until the timeout is reached.
            The timeout specifies the number of seconds to wait between log entries before terminating the stream.
            This method will stop when the FunctionCall is observed to have completed,
            or when the timeout is reached. The completion check is best-effort; if completion
            cannot be determined, the stream will continue until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                   By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.stream():
                    print(entry.message, end="")
                ```
            """
            ...

    stream: __stream_spec

    class __tail_spec(typing_extensions.Protocol):
        def __call__(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch the most recent FunctionCall logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.tail(entries=10):
                    print(entry.timestamp, entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch the most recent FunctionCall logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.tail(entries=10):
                    print(entry.timestamp, entry.message, end="")
                ```
            """
            ...

    tail: __tail_spec

    class __fetch_spec(typing_extensions.Protocol):
        def __call__(
            self,
            /,
            *,
            since: typing.Optional[datetime.datetime] = None,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch all associated logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                    By default, this will fetch logs from the start of the function call.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.fetch():
                    print(entry.timestamp, entry.message, end="")
                ```
            """
            ...

        def aio(
            self,
            /,
            *,
            since: typing.Optional[datetime.datetime] = None,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch all associated logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                    By default, this will fetch logs from the start of the function call.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                function = modal.Function.from_name("my-app", "train")
                call = function.spawn()

                for entry in call.logs.fetch():
                    print(entry.timestamp, entry.message, end="")
                ```
            """
            ...

    fetch: __fetch_spec

class _ImageLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsImageLogs):
        """mdmd:hidden"""
        ...

    def fetch(self, layers: typing.Optional[int] = 1) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch logs for the most recent Image build steps.

        Args:
            layers: The number of build layers to fetch, counting backward
                from the final Image. If None, logs are fetched for all build steps.
        """
        ...

    def tail(self, entries: int = 100) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent Image logs.

        Args:
            entries: The number of log entries to return.
        """
        ...

class ImageLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsImageLogs):
        """mdmd:hidden"""
        ...

    class __fetch_spec(typing_extensions.Protocol):
        def __call__(self, /, layers: typing.Optional[int] = 1) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch logs for the most recent Image build steps.

            Args:
                layers: The number of build layers to fetch, counting backward
                    from the final Image. If None, logs are fetched for all build steps.
            """
            ...

        def aio(
            self, /, layers: typing.Optional[int] = 1
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch logs for the most recent Image build steps.

            Args:
                layers: The number of build layers to fetch, counting backward
                    from the final Image. If None, logs are fetched for all build steps.
            """
            ...

    fetch: __fetch_spec

    class __tail_spec(typing_extensions.Protocol):
        def __call__(self, /, entries: int = 100) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch the most recent Image logs.

            Args:
                entries: The number of log entries to return.
            """
            ...

        def aio(self, /, entries: int = 100) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch the most recent Image logs.

            Args:
                entries: The number of log entries to return.
            """
            ...

    tail: __tail_spec

class _AppLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    def fetch(
        self,
        *,
        since: datetime.datetime,
        until: typing.Optional[datetime.datetime] = None,
        source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
        search_text: str = "",
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch App logs corresponding to the date range and filters.

        Args:
            since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                which is interpreted as local time.
            until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                as local time.
            source: Filter by source: 'stdout', 'stderr', or 'system'.
            search_text: Filter by search text.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            app = modal.App.lookup("my-app")

            for entry in app.logs.fetch(
                since=datetime.now() - timedelta(hours=4),
                source="stdout",
            ):
                print(entry.message, end="")
            ```
        """
        ...

    def tail(
        self, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Fetch the most recent App logs.

        Args:
            entries: The number of log entries to return.
            source: Filter by source: 'stdout', 'stderr', or 'system'.

        Yields:
            `LogEntry` objects in chronological order.

        Examples:

            ```python notest
            app = modal.App.lookup("my-app")

            for entry in app.logs.tail(20):
                print(entry.message, end="")
            ```
        """
        ...

    def stream(
        self, timeout: typing.Optional[float] = None
    ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
        """Stream new App logs until the timeout is reached.

        Args:
            timeout: Number of seconds to wait between log entries before terminating the stream.
                By default, this will block until it is interrupted.

        Yields:
            `LogEntry` objects as they arrive.

        Examples:

            ```python notest
            app = modal.App.lookup("my-app")

            for entry in app.logs.stream(timeout=60):
                print(entry.message, end="")
            ```
        """
        ...

class AppLogsManager:
    """mdmd:namespace"""
    def __init__(self, source: modal._supports_logs._SupportsLogs):
        """mdmd:hidden"""
        ...

    class __fetch_spec(typing_extensions.Protocol):
        def __call__(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch App logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.fetch(
                    since=datetime.now() - timedelta(hours=4),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self,
            /,
            *,
            since: datetime.datetime,
            until: typing.Optional[datetime.datetime] = None,
            source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None,
            search_text: str = "",
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch App logs corresponding to the date range and filters.

            Args:
                since: Start date to fetch logs from. Must be in UTC or timezone-naive,
                    which is interpreted as local time.
                until: Defaults to current date if None. Must be in UTC or timezone-naive, which is interpreted
                    as local time.
                source: Filter by source: 'stdout', 'stderr', or 'system'.
                search_text: Filter by search text.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.fetch(
                    since=datetime.now() - timedelta(hours=4),
                    source="stdout",
                ):
                    print(entry.message, end="")
                ```
            """
            ...

    fetch: __fetch_spec

    class __tail_spec(typing_extensions.Protocol):
        def __call__(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Fetch the most recent App logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, entries: int = 100, *, source: typing.Optional[typing.Literal["stdout", "stderr", "system"]] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Fetch the most recent App logs.

            Args:
                entries: The number of log entries to return.
                source: Filter by source: 'stdout', 'stderr', or 'system'.

            Yields:
                `LogEntry` objects in chronological order.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.tail(20):
                    print(entry.message, end="")
                ```
            """
            ...

    tail: __tail_spec

    class __stream_spec(typing_extensions.Protocol):
        def __call__(
            self, /, timeout: typing.Optional[float] = None
        ) -> typing.Generator[modal.types.LogEntry, None, None]:
            """Stream new App logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

        def aio(
            self, /, timeout: typing.Optional[float] = None
        ) -> collections.abc.AsyncGenerator[modal.types.LogEntry, None]:
            """Stream new App logs until the timeout is reached.

            Args:
                timeout: Number of seconds to wait between log entries before terminating the stream.
                    By default, this will block until it is interrupted.

            Yields:
                `LogEntry` objects as they arrive.

            Examples:

                ```python notest
                app = modal.App.lookup("my-app")

                for entry in app.logs.stream(timeout=60):
                    print(entry.message, end="")
                ```
            """
            ...

    stream: __stream_spec
