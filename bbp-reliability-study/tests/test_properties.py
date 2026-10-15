import pytest
hyp = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st
from bbp_reliability import FakeAdapter, Delta, Fault, Done, ErrorClass, RunStore
from helpers import run, collect_frames

step = st.one_of(st.text(min_size=1, max_size=5).map(Delta),
                 st.sampled_from(list(ErrorClass)).map(Fault), st.just(Done()))


@given(st.lists(st.lists(step, max_size=6), min_size=1, max_size=3))
def test_exactly_one_terminal_state_and_text_matches_delivery(attempts):
    s = RunStore(); rid = s.create_run("c", "q")
    from bbp_reliability import FakeClock
    clock = FakeClock()
    frames = run(collect_frames(s, FakeAdapter(*attempts), rid, sleep=clock.sleep))
    state, _ = s.run(rid)
    assert state in {"COMPLETED", "FAILED", "ABORTED"}
    assert s.message_count(rid) == 2                      # user + exactly one assistant message
    delivered = "".join(__import__("json").loads(f.split("data: ")[1])["text"]
                        for f in frames if f.startswith("event: delta"))
    assert s.assistant_text(rid) == delivered
    assert sum(f.startswith(("event: done", "event: error")) for f in frames) == 1
