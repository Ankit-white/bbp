"""Reproduces the measurements in the technical report (Section 6). Mock provider only; timings are machine-dependent.
Run: python examples/evaluate.py   (writes evaluation_results.json in the current directory)"""
import asyncio, sys, time, json, statistics, platform
import pathlib; sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent.parent/"src"))
from bbp_reliability import *
from bbp_reliability.lifecycle import run_stream as rs

SC = {
 "success":[[Delta("a"),Delta("b"),Delta("c"),Done()]],
 "retry_pre_token":[[Fault(ErrorClass.RATE_LIMIT,retry_after_s=2.0)],[Delta("x"),Done()]],
 "midstream_fault":[[Delta("a"),Delta("b"),Fault(ErrorClass.UNAVAILABLE)]],
 "auth":[[Fault(ErrorClass.AUTH)]],
 "truncated":[[Delta("a"),Delta("b")]],
}
async def play(attempts, seed=7):
    import random
    s=RunStore(); rid=s.create_run("c","q"); clk=FakeClock()
    frames=[f async for f in rs(s,FakeAdapter(*attempts),{},rid,sleep=clk.sleep,rng=random.Random(seed))]
    return (frames, s.run(rid), s.assistant_text(rid), tuple(clk.sleeps))
async def determinism(N=1000):
    out={}
    for n,a in SC.items():
        base=await play(a); same=0
        for _ in range(N):
            same += (await play(a))==base
        out[n]=(same,N)
    return out
async def classification():
    ok=0
    for c in ErrorClass:
        if c==ErrorClass.CANCELLED: continue
        s=RunStore(); rid=s.create_run("c","q")
        fr=[f async for f in rs(s,FakeAdapter([Fault(c)]),{},rid,sleep=FakeClock().sleep)]
        ok += (s.run(rid)==("FAILED",c.value) and f'"{c.value}"' in fr[-1])
    return ok, len(ErrorClass)-1
async def abort_sweep():
    res={"cases":0,"ok":0,"pending_before_start":0}
    scripts=[[Delta("a"),Delta("b"),Delta("c"),Done()],[Delta("a"),Fault(ErrorClass.TIMEOUT)],
             [Fault(ErrorClass.RATE_LIMIT,retry_after_s=1.0)]+[Delta("q"),Done()] ]
    for sc in scripts:
        att=[sc] if not isinstance(sc[0],Fault) else [sc[:1],sc[1:]]
        for k in range(0,6):   # close after k frames
            s=RunStore(); rid=s.create_run("c","q")
            g=rs(s,FakeAdapter(*att),{},rid,sleep=FakeClock().sleep)
            got=[]
            try:
                for _ in range(k): got.append(await g.__anext__())
            except StopAsyncIteration: pass
            await g.aclose()
            st=s.run(rid)[0]
            if k==0:
                res["pending_before_start"] += (st=="PENDING"); continue
            res["cases"]+=1
            delivered="".join(__import__("json").loads(f.split("data: ")[1])["text"] for f in got if f.startswith("event: delta"))
            terminal_seen=any(f.startswith(("event: done","event: error")) for f in got)
            good = st in ("COMPLETED","FAILED","ABORTED") and s.message_count(rid)==2 and s.assistant_text(rid)==delivered
            res["ok"]+=good
    # task cancellation while adapter is awaiting
    class Slow:
        name="slow"
        async def stream(self,req):
            yield StreamEvent.delta("partial"); await asyncio.sleep(10); yield StreamEvent.done()
    s=RunStore(); rid=s.create_run("c","q")
    async def consume():
        async for _ in rs(s,Slow(),{},rid): pass
    t=asyncio.create_task(consume()); await asyncio.sleep(0.05); t.cancel()
    try: await t
    except asyncio.CancelledError: pass
    res["task_cancel"]=(s.run(rid),s.assistant_text(rid))
    return res
async def bench(N=3000):
    class Direct:
        async def run(self):
            async for ev in FakeAdapter([Delta("a"),Done()]).stream({}): return ev
    d=[];w=[]
    for _ in range(N):
        t=time.perf_counter_ns(); await Direct().run(); d.append(time.perf_counter_ns()-t)
        s=RunStore(); rid=s.create_run("c","q")
        t=time.perf_counter_ns()
        g=rs(s,FakeAdapter([Delta("a"),Done()]),{},rid); await g.__anext__()
        w.append(time.perf_counter_ns()-t); await g.aclose()
    q=lambda x,p: sorted(x)[int(len(x)*p)-1]/1000
    return {k:(round(q(v,.5),1),round(q(v,.95),1),round(q(v,.99),1)) for k,v in (("direct",d),("pipeline",w))}
async def main():
    r={"python":platform.python_version(),"machine":platform.machine(),"os":platform.system()}
    r["determinism"]=await determinism(); r["classification"]=await classification()
    r["abort"]=await abort_sweep(); r["bench_us_p50_p95_p99"]=await bench()
    print(json.dumps(r,indent=1,default=str)); json.dump(r,open("evaluation_results.json","w"),default=str)
asyncio.run(main())
