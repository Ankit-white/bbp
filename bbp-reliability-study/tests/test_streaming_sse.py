from bbp_reliability import FakeAdapter, Delta, Fault, Done, ErrorClass, RunStore
from helpers import run, collect_frames


def test_successful_stream_ends_with_done():
    s = RunStore(); rid = s.create_run("c1", "hi")
    frames = run(collect_frames(s, FakeAdapter([Delta("a"), Delta("b"), Done()]), rid))
    assert frames[-1].startswith("event: done") and len(frames) == 3


def test_midstream_error_is_last_event_and_hides_detail():
    s = RunStore(); rid = s.create_run("c1", "hi")
    frames = run(collect_frames(s, FakeAdapter([Delta("a"), Fault(ErrorClass.RATE_LIMIT)]), rid))
    assert frames[-1].startswith("event: error") and '"rate_limit"' in frames[-1]
    assert "detail" not in frames[-1]
