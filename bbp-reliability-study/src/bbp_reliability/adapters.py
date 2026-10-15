"""Adapter protocol plus a scripted fake provider for deterministic fault injection."""
from dataclasses import dataclass
from typing import AsyncIterator, List, Optional, Protocol, Union
from .errors import ErrorClass, InferenceError
from .events import StreamEvent


class ProviderAdapter(Protocol):
    name: str
    def stream(self, req: dict) -> AsyncIterator[StreamEvent]: ...


class FakeClock:
    """Injected clock: records requested sleeps instead of sleeping."""
    def __init__(self): self.sleeps: List[float] = []
    async def sleep(self, s: float) -> None: self.sleeps.append(s)


@dataclass
class Delta:
    text: str
    delay_s: float = 0.0

@dataclass
class Fault:
    cls: ErrorClass
    retry_after_s: Optional[float] = None

@dataclass
class Done:
    pass

Step = Union[Delta, Fault, Done]


class FakeAdapter:
    """Each call to stream() plays the next script, so a scenario can describe several attempts."""
    name = "fake"

    def __init__(self, *attempts: List[Step], clock: Optional[FakeClock] = None):
        self.attempts = list(attempts)
        self.calls = 0
        self.clock = clock

    async def stream(self, req: dict) -> AsyncIterator[StreamEvent]:
        script = self.attempts[min(self.calls, len(self.attempts) - 1)]
        self.calls += 1
        for step in script:
            if isinstance(step, Delta):
                if step.delay_s and self.clock:
                    await self.clock.sleep(step.delay_s)
                yield StreamEvent.delta(step.text)
            elif isinstance(step, Fault):
                yield StreamEvent.fail(InferenceError.of(step.cls, step.retry_after_s))
                return
            elif isinstance(step, Done):
                yield StreamEvent.done()
                return
        # a script that ends without a terminal step simulates a truncated stream
