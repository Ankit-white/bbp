from dataclasses import dataclass
from typing import Optional
from .errors import InferenceError


@dataclass(frozen=True)
class StreamEvent:
    """Normalised event vocabulary. Contract: zero or more 'delta', then exactly one 'done' or 'error'."""
    kind: str  # "delta" | "usage" | "done" | "error"
    text: Optional[str] = None
    usage: Optional[dict] = None
    error: Optional[InferenceError] = None

    @staticmethod
    def delta(text: str) -> "StreamEvent": return StreamEvent("delta", text=text)
    @staticmethod
    def done() -> "StreamEvent": return StreamEvent("done")
    @staticmethod
    def fail(err: InferenceError) -> "StreamEvent": return StreamEvent("error", error=err)

    @property
    def terminal(self) -> bool:
        return self.kind in ("done", "error")
