import random
from bbp_reliability import FakeAdapter, FakeClock, Delta, Fault, Done, ErrorClass
from helpers import run, collect_events


def kinds(evs): return [e.kind for e in evs]


def test_retry_before_first_token_succeeds():
    clock = FakeClock()
    a = FakeAdapter([Fault(ErrorClass.UNAVAILABLE)], [Delta("hi"), Done()])
    evs = run(collect_events(a, sleep=clock.sleep))
    assert kinds(evs) == ["delta", "done"] and a.calls == 2 and len(clock.sleeps) == 1


def test_retry_after_hint_is_honoured():
    clock = FakeClock()
    a = FakeAdapter([Fault(ErrorClass.RATE_LIMIT, retry_after_s=2.0)], [Delta("x"), Done()])
    run(collect_events(a, sleep=clock.sleep))
    assert clock.sleeps == [2.0]


def test_no_retry_after_first_delta():
    clock = FakeClock()
    a = FakeAdapter([Delta("Hello"), Fault(ErrorClass.RATE_LIMIT, retry_after_s=2.0)], [Delta("again"), Done()])
    evs = run(collect_events(a, sleep=clock.sleep))
    assert kinds(evs) == ["delta", "error"] and a.calls == 1 and clock.sleeps == []


def test_non_retryable_errors_fail_fast():
    clock = FakeClock()
    a = FakeAdapter([Fault(ErrorClass.AUTH)], [Delta("x"), Done()])
    evs = run(collect_events(a, sleep=clock.sleep))
    assert kinds(evs) == ["error"] and evs[0].error.cls == ErrorClass.AUTH and a.calls == 1


def test_retry_count_is_bounded():
    clock = FakeClock()
    a = FakeAdapter([Fault(ErrorClass.UNAVAILABLE)])
    evs = run(collect_events(a, sleep=clock.sleep, max_retries=2))
    assert a.calls == 3 and len(clock.sleeps) == 2 and evs[-1].kind == "error"


def test_backoff_is_deterministic_with_seed():
    def delays():
        clock = FakeClock()
        run(collect_events(FakeAdapter([Fault(ErrorClass.UNAVAILABLE)]), sleep=clock.sleep,
                           rng=random.Random(42)))
        return clock.sleeps
    assert delays() == delays()


def test_truncated_stream_becomes_protocol_error():
    evs = run(collect_events(FakeAdapter([Delta("partial")])))
    assert kinds(evs) == ["delta", "error"] and evs[-1].error.cls == ErrorClass.PROTOCOL
