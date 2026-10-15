"""Retry policy: retry only before the first delta; honour retry-after; jitter from an injected RNG."""
import asyncio
import random
from contextlib import aclosing
from typing import AsyncIterator, Awaitable, Callable, Optional
from .errors import ErrorClass, InferenceError
from .events import StreamEvent


async def execute(adapter, req: dict, *, max_retries: int = 2, base_delay_s: float = 0.5,
                  sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                  rng: Optional[random.Random] = None) -> AsyncIterator[StreamEvent]:
    rng = rng or random.Random(0)
    attempt = 0
    while True:
        seen_delta = False
        retry = False
        async with aclosing(adapter.stream(req)) as events:
            async for ev in events:
                if ev.kind == "delta":
                    seen_delta = True
                if ev.kind == "error":
                    err: InferenceError = ev.error
                    if err.retryable and not seen_delta and attempt < max_retries:
                        delay = err.retry_after_s if err.retry_after_s is not None \
                            else base_delay_s * (2 ** attempt) * (1 + 0.1 * rng.random())
                        attempt += 1
                        await sleep(delay)
                        retry = True
                        break
                    yield ev
                    return
                yield ev
                if ev.kind == "done":
                    return
        if retry:
            continue
        # stream ended without a terminal event: truncated stream
        yield StreamEvent.fail(InferenceError.of(ErrorClass.PROTOCOL, detail="stream ended without terminal event"))
        return
