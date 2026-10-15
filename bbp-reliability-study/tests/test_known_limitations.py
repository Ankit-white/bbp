"""Documents a known limitation instead of hiding it (see report, Section 6)."""
from bbp_reliability import FakeAdapter, Delta, Done, RunStore, run_stream
from helpers import run


def test_disconnect_before_first_frame_leaves_run_pending():
    """If the consumer closes the generator before it ever starts, the generator body (and its
    `finally` block) never runs, so the run stays PENDING. Mitigation (not implemented here):
    a periodic reaper that marks stale PENDING runs as ABORTED."""
    async def scenario():
        s = RunStore(); rid = s.create_run("c1", "hi")
        gen = run_stream(s, FakeAdapter([Delta("x"), Done()]), {}, rid)
        await gen.aclose()
        return s.run(rid)
    assert run(scenario()) == ("PENDING", None)
