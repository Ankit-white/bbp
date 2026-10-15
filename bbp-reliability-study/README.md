# bbp-reliability-study

Supporting material for the SciPy India 2026 talk **"Engineering Reliable AI Inference: A Python-Based Case Study in Streaming, Error Handling, and Reproducibility."**

This repository is a small, self-contained reference implementation of the patterns described in the technical report. It uses **mock providers only**: no network access, no API keys, no production code.

## What it demonstrates

| Concern | Where |
|---|---|
| Provider-independent event vocabulary | `src/bbp_reliability/events.py`, `adapters.py` |
| Closed error taxonomy and HTTP mapping | `src/bbp_reliability/errors.py` |
| Retry only before the first token, `retry-after`, seeded jitter | `src/bbp_reliability/orchestrator.py` |
| SSE framing with in-band errors (no internal detail leaked) | `src/bbp_reliability/sse.py` |
| Run lifecycle, exactly-once finalisation, abort as a first-class state | `src/bbp_reliability/lifecycle.py` |
| Deterministic fault injection (scripted fake provider, fake clock) | `src/bbp_reliability/adapters.py`, `tests/` |

## Layout

```
docs/       architecture.png, technical-report.pdf
src/        the reference implementation (standard library only)
tests/      public, runnable tests for error handling and streaming behaviour
examples/   demo.py: minimal demonstration with mock AI providers
```

## Run it

Requires Python 3.10+.

```bash
python examples/demo.py          # five scenarios, printed as SSE frames
pip install -e ".[dev]"          # pytest and hypothesis
pytest                           # unit, lifecycle and property-based tests
python examples/evaluate.py      # reproduces the report's measurements (mock provider)
```

The runtime library has no third-party dependencies. `hypothesis` is optional; the property-based test is skipped if it is not installed.

## Scope and simplifications

- SQLite stands in for PostgreSQL so the tests run anywhere. The invariants rely only on a `UNIQUE` constraint and a guarded `UPDATE`, both available in PostgreSQL.
- The FastAPI layer is omitted; `run_stream` yields SSE frames that any ASGI framework can return in a streaming response.
- Real provider adapters are intentionally not included. An adapter's only job is to translate a provider's wire format and errors into `StreamEvent` and `InferenceError`.

## Known limitations

- A client that disconnects before receiving any frame leaves the run in `PENDING` (the generator never starts, so its `finally` block never runs). `tests/test_known_limitations.py` documents this; a reaper for stale `PENDING` runs is the intended mitigation and is not implemented.
- Timings from `evaluate.py` measure the abstraction layer against a zero-latency fake provider on one machine. They do not predict end-to-end latency.

## Reproducibility

Time and randomness are injected (`FakeClock`, seeded `random.Random`), so retry and backoff behaviour is asserted without sleeping, and every fault scenario is plain data in the test file.

## Licence

MIT. See `LICENSE`.
