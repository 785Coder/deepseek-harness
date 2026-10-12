"""Typed, non-consuming job observation independent of session notification queues."""
from __future__ import annotations

import queue
import threading
from collections import deque
from typing import Callable, Iterator, Literal, TypeAlias, TypeVar, Generic

from pydantic import BaseModel, ConfigDict, Field

from .errors import HarnessError, JsonRpcError, SdkProtocolError, TransportClosedError


class JobSubscriptionError(HarnessError):
    """Observation failed without changing the task itself."""


class JobAccessError(JobSubscriptionError):
    """Session is not owned by this SDK client."""


class JobNotFoundError(JobSubscriptionError):
    """Task is absent or not visible."""


class JobCursorError(JobSubscriptionError):
    """Output cursor is invalid."""


class JobServiceUnavailableError(JobSubscriptionError):
    """Runtime has no jobs service."""


class JobRemovedError(JobSubscriptionError):
    """Task was removed; remaining output cannot be guaranteed."""


def job_error(error: JsonRpcError) -> HarnessError:
    data: object = error.data
    kind: object = data.get("kind") if isinstance(data, dict) else None
    classes: dict[str, type[JobSubscriptionError]] = {
        "session-not-owned": JobAccessError, "job-not-found": JobNotFoundError,
        "invalid-cursor": JobCursorError, "jobs-unavailable": JobServiceUnavailableError,
    }
    if isinstance(kind, str):
        return classes.get(kind, JobSubscriptionError)(error.message)
    return error


def _job_closed_error(error: object) -> JobSubscriptionError:
    """Build a typed error from a `job.closed` frame's optional `error` member."""
    if not isinstance(error, dict):
        return JobSubscriptionError(str(error) if error is not None else "job observation failed")
    kind: object = error.get("kind")
    message: object = error.get("message")
    text: str = str(message) if isinstance(message, str) else "job observation failed"
    if isinstance(kind, str):
        return JobSubscriptionError(f"{text} ({kind})")
    return JobSubscriptionError(text)


class JobModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class JobOutputCoordinates(JobModel):
    total: int
    earliest: int
    spill_paths: list[str] | None = Field(default=None, alias="spillPaths")


class JobView(JobModel):
    id: str
    kind: str
    label: str
    owner: str | None = None
    status: Literal["running", "stopping", "completed", "killed", "failed"]
    progress: str | None = None
    detail: str | None = None
    started_at: int = Field(alias="startedAt")
    finished_at: int | None = Field(default=None, alias="finishedAt")
    output_limit_bytes: int | None = Field(default=None, alias="outputLimitBytes")
    output: JobOutputCoordinates


class JobChunk(JobModel):
    at: int
    text: str
    channel: str | None = None
    gap_before: bool = Field(default=False, alias="gapBefore")


class JobOutputRead(JobModel):
    job: JobView
    chunks: list[JobChunk]
    next: int
    lossy: bool


class JobRowsFrame(JobModel):
    type: Literal["rows"]
    jobs: list[JobView]


class JobOpenedFrame(JobModel):
    type: Literal["opened"]
    job: JobView
    from_offset: int = Field(alias="from")


class JobOutputFrame(JobModel):
    type: Literal["output"]
    chunks: list[JobChunk]
    next: int
    lossy: bool = False


class JobStatusFrame(JobModel):
    type: Literal["status"]
    job: JobView


JobFollowFrame: TypeAlias = JobOpenedFrame | JobOutputFrame | JobStatusFrame
FrameT = TypeVar("FrameT", JobRowsFrame, JobFollowFrame)


class JobSubscription(Generic[FrameT]):
    """One consumer, one buffered frame; termination lives outside the buffer."""

    def __init__(self, subscription_id: str, parse: Callable[[object], FrameT],
                 acknowledge: Callable[[str, int], None], cancel: Callable[[str], None]) -> None:
        self.subscription_id: str = subscription_id
        self._parse: Callable[[object], FrameT] = parse
        self._acknowledge: Callable[[str, int], None] = acknowledge
        self._cancel: Callable[[str], None] = cancel
        self._condition: threading.Condition = threading.Condition()
        self._frames: deque[tuple[int, FrameT]] = deque()
        self._ended: bool = False
        self._error: BaseException | None = None
        self._last_seq: int = 0
        self._locally_closed: bool = False

    def __enter__(self) -> JobSubscription[FrameT]:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def _receive(self, seq: object, frame: object) -> None:
        try:
            if not isinstance(seq, int) or isinstance(seq, bool) or seq != self._last_seq + 1:
                raise SdkProtocolError("Invalid job frame sequence")
            parsed: FrameT = self._parse(frame)
            with self._condition:
                if self._ended:
                    return
                if self._frames:
                    raise SdkProtocolError("Job subscription credit window exceeded")
                self._last_seq = seq
                self._frames.append((seq, parsed))
                self._condition.notify_all()
        except Exception as error:
            self._finish(error)

    def _finish(self, error: BaseException | None = None) -> None:
        with self._condition:
            if self._ended:
                return
            self._ended = True
            self._error = error
            self._condition.notify_all()

    def next(self, timeout: float | None = None) -> FrameT:
        """Return a typed frame; timeout raises queue.Empty without closing observation."""
        with self._condition:
            if not self._condition.wait_for(lambda: bool(self._frames) or self._ended, timeout):
                raise queue.Empty
            if not self._frames:
                if self._error is not None:
                    raise self._error
                raise StopIteration
            seq: int
            frame: FrameT
            seq, frame = self._frames.popleft()
        # A cached frame remains readable after transport termination.
        with self._condition:
            ended: bool = self._ended
        if not ended:
            try:
                self._acknowledge(self.subscription_id, seq)
            except TransportClosedError as error:
                self._finish(error)
        return frame

    def close(self) -> None:
        """Stop observing without cancelling the job; wake blocked consumers."""
        with self._condition:
            if self._locally_closed:
                return
            self._locally_closed = True
            self._frames.clear()
            self._ended = True
            self._error = None
            self._condition.notify_all()
        self._cancel(self.subscription_id)

    def __iter__(self) -> Iterator[FrameT]:
        while True:
            try:
                yield self.next()
            except StopIteration:
                return


class JobRowsSubscription(JobSubscription[JobRowsFrame]):
    """Whole-set job list observation."""


class JobOutputSubscription(JobSubscription[JobFollowFrame]):
    """Retained output followed by terminal task status."""


def parse_rows(value: object) -> JobRowsFrame:
    return JobRowsFrame.model_validate(value)


def parse_output(value: object) -> JobFollowFrame:
    kind: object = value.get("type") if isinstance(value, dict) else None
    if kind == "opened":
        return JobOpenedFrame.model_validate(value)
    if kind == "output":
        return JobOutputFrame.model_validate(value)
    if kind == "status":
        return JobStatusFrame.model_validate(value)
    raise SdkProtocolError("Unknown job output frame")


class JobListResult(JobModel):
    jobs: list[JobView]


class JobSubscriptionReceipt(JobModel):
    subscription_id: str = Field(alias="subscriptionId")
