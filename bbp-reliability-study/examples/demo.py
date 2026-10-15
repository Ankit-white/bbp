"""Run: python examples/demo.py   (no network, no API keys; uses mock providers only)"""
import asyncio, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
from bbp_reliability import FakeAdapter, FakeClock, Delta, Fault, Done, ErrorClass, RunStore, run_stream

SCENARIOS = {
    "1. success": [[Delta("Hello"), Delta(" world"), Done()]],
    "2. retry before first token": [[Fault(ErrorClass.RATE_LIMIT, retry_after_s=2.0)], [Delta("recovered"), Done()]],
    "3. mid-stream rate limit (not retried)": [[Delta("Hello"), Delta(" wor"), Fault(ErrorClass.RATE_LIMIT)]],
    "4. authentication failure": [[Fault(ErrorClass.AUTH)]],
}


async def main():
    for name, attempts in SCENARIOS.items():
        store, clock = RunStore(), FakeClock()
        rid = store.create_run("demo", "question")
        print(f"\n=== {name}")
        async for frame in run_stream(store, FakeAdapter(*attempts), {}, rid, sleep=clock.sleep):
            print(frame.strip().replace("\n", "  |  "))
        print("state:", store.run(rid), "| stored text:", repr(store.assistant_text(rid)), "| sleeps:", clock.sleeps)

    # client disconnect after first frame
    store = RunStore(); rid = store.create_run("demo", "question")
    gen = run_stream(store, FakeAdapter([Delta("one"), Delta("two"), Done()]), {}, rid)
    print("\n=== 5. client disconnect"); print((await gen.__anext__()).strip().replace("\n", "  |  "))
    await gen.aclose()
    print("state:", store.run(rid), "| stored text:", repr(store.assistant_text(rid)))

asyncio.run(main())
