import asyncio, random
from bbp_reliability import FakeClock, run_stream, execute


def run(coro):
    return asyncio.run(coro)


async def collect_events(adapter, **kw):
    return [ev async for ev in execute(adapter, {}, **kw)]


async def collect_frames(store, adapter, run_id, **kw):
    return [f async for f in run_stream(store, adapter, {}, run_id, **kw)]
