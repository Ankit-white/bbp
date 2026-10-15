import json
from .events import StreamEvent


def sse_frame(ev: StreamEvent) -> str:
    if ev.kind == "delta":
        payload = {"text": ev.text}
    elif ev.kind == "error":
        payload = {"class": ev.error.cls.value, "message": ev.error.public_message}  # never ev.error.detail
    else:
        payload = {}
    return f"event: {ev.kind}\ndata: {json.dumps(payload)}\n\n"
