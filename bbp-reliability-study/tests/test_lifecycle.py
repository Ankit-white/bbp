import asyncio
from bbp_reliability import FakeAdapter, Delta, Fault, Done, ErrorClass, RunStore, run_stream
from helpers import run, collect_frames


def test_user_message_is_durable_before_any_provider_call():
    s = RunStore(); rid = s.create_run("c1", "hello")
    assert s.run(rid) == ("PENDING", None) and s.message_count(rid) == 1


def test_completed_run_persists_full_text():
    s = RunStore(); rid = s.create_run("c1", "hi")
    run(collect_frames(s, FakeAdapter([Delta("Hello"), Delta(" world"), Done()]), rid))
    assert s.run(rid) == ("COMPLETED", None) and s.assistant_text(rid) == "Hello world"


def test_midstream_failure_keeps_partial_text():
    s = RunStore(); rid = s.create_run("c1", "hi")
    run(collect_frames(s, FakeAdapter([Delta("Hello"), Delta(" wor"), Fault(ErrorClass.UNAVAILABLE)]), rid))
    assert s.run(rid) == ("FAILED", "unavailable") and s.assistant_text(rid) == "Hello wor"


def test_pre_stream_failure_is_failed_with_empty_text():
    s = RunStore(); rid = s.create_run("c1", "hi")
    run(collect_frames(s, FakeAdapter([Fault(ErrorClass.AUTH)]), rid))
    assert s.run(rid) == ("FAILED", "auth") and s.assistant_text(rid) == ""


def test_client_disconnect_is_aborted_not_failed_and_keeps_delivered_text():
    async def scenario():
        s = RunStore(); rid = s.create_run("c1", "hi")
        gen = run_stream(s, FakeAdapter([Delta("one"), Delta("two"), Done()]), {}, rid)
        await gen.__anext__()          # client receives first frame, then disconnects
        await gen.aclose()
        return s, rid
    s, rid = run(scenario())
    assert s.run(rid) == ("ABORTED", "cancelled") and s.assistant_text(rid) == "one"


def test_finalization_is_exactly_once():
    s = RunStore(); rid = s.create_run("c1", "hi")
    assert s.finalize(rid, "a", "COMPLETED", None) is True
    assert s.finalize(rid, "b", "FAILED", "internal") is False
    assert s.assistant_text(rid) == "a" and s.message_count(rid) == 2  # user + one assistant
