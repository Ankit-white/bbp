"""Minimal, provider-independent reference implementation of the patterns in the technical report."""
from .errors import ErrorClass, InferenceError, HTTP_STATUS, classify_http_status
from .events import StreamEvent
from .adapters import FakeAdapter, Delta, Fault, Done, FakeClock
from .orchestrator import execute
from .lifecycle import RunStore, run_stream
from .sse import sse_frame

__all__ = ["ErrorClass", "InferenceError", "HTTP_STATUS", "classify_http_status", "StreamEvent",
           "FakeAdapter", "Delta", "Fault", "Done", "FakeClock", "execute", "RunStore",
           "run_stream", "sse_frame"]
