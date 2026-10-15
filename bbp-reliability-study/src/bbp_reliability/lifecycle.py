"""Run lifecycle with exactly-once finalisation. SQLite stands in for PostgreSQL in this public example;
the invariants rely only on a UNIQUE constraint and a guarded UPDATE, which PostgreSQL provides as well."""
import asyncio
import sqlite3
from typing import AsyncIterator, Optional, Tuple
from .errors import ErrorClass
from .events import StreamEvent
from .orchestrator import execute
from .sse import sse_frame

PENDING, STREAMING, COMPLETED, FAILED, ABORTED = "PENDING", "STREAMING", "COMPLETED", "FAILED", "ABORTED"


class RunStore:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript("""
        CREATE TABLE runs(id INTEGER PRIMARY KEY, conv_id TEXT, state TEXT, error_class TEXT);
        CREATE TABLE messages(id INTEGER PRIMARY KEY, conv_id TEXT, role TEXT, text TEXT, run_id INTEGER,
                              UNIQUE(run_id, role));
        """)

    def create_run(self, conv_id: str, user_text: str) -> int:
        with self.db:  # committed before any provider call
            cur = self.db.execute("INSERT INTO runs(conv_id,state) VALUES(?,?)", (conv_id, PENDING))
            run_id = cur.lastrowid
            self.db.execute("INSERT INTO messages(conv_id,role,text,run_id) VALUES(?,?,?,?)",
                            (conv_id, "user", user_text, run_id))
        return run_id

    def mark_streaming(self, run_id: int) -> None:
        with self.db:
            self.db.execute("UPDATE runs SET state=? WHERE id=? AND state=?", (STREAMING, run_id, PENDING))

    def finalize(self, run_id: int, text: str, state: str, error_class: Optional[str]) -> bool:
        """Returns True only for the call that performed the transition (exactly-once)."""
        with self.db:
            cur = self.db.execute(
                "UPDATE runs SET state=?, error_class=? WHERE id=? AND state IN (?,?)",
                (state, error_class, run_id, PENDING, STREAMING))
            if cur.rowcount == 0:
                return False
            conv = self.db.execute("SELECT conv_id FROM runs WHERE id=?", (run_id,)).fetchone()[0]
            self.db.execute("INSERT INTO messages(conv_id,role,text,run_id) VALUES(?,?,?,?)",
                            (conv, "assistant", text, run_id))
            return True

    def run(self, run_id: int) -> Tuple[str, Optional[str]]:
        return self.db.execute("SELECT state, error_class FROM runs WHERE id=?", (run_id,)).fetchone()

    def assistant_text(self, run_id: int) -> Optional[str]:
        row = self.db.execute("SELECT text FROM messages WHERE run_id=? AND role='assistant'", (run_id,)).fetchone()
        return row[0] if row else None

    def message_count(self, run_id: int) -> int:
        return self.db.execute("SELECT COUNT(*) FROM messages WHERE run_id=?", (run_id,)).fetchone()[0]


async def _finalize(store: RunStore, run_id: int, text: str, terminal: Optional[StreamEvent], aborted: bool) -> None:
    if aborted or (terminal and terminal.kind == "error" and terminal.error.cls == ErrorClass.CANCELLED):
        store.finalize(run_id, text, ABORTED, ErrorClass.CANCELLED.value)
    elif terminal is None:
        store.finalize(run_id, text, FAILED, ErrorClass.PROTOCOL.value)
    elif terminal.kind == "done":
        store.finalize(run_id, text, COMPLETED, None)
    else:
        store.finalize(run_id, text, FAILED, terminal.error.cls.value)


async def run_stream(store: RunStore, adapter, req: dict, run_id: int, **orch_kwargs) -> AsyncIterator[str]:
    """Yields SSE frames. Every exit path (done, error, client disconnect) finalises exactly once."""
    buffer, terminal, aborted, started = [], None, False, False
    try:
        async for ev in execute(adapter, req, **orch_kwargs):
            if not started:
                store.mark_streaming(run_id); started = True
            if ev.kind == "delta":
                buffer.append(ev.text)
            if ev.terminal:
                terminal = ev
            yield sse_frame(ev)
            if terminal:
                break
    except (asyncio.CancelledError, GeneratorExit):
        aborted = True
        raise
    finally:
        await asyncio.shield(_finalize(store, run_id, "".join(buffer), terminal, aborted))
