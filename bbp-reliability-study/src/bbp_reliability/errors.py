"""Closed error taxonomy. Adapters translate provider-specific failures into these classes."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ErrorClass(str, Enum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    INVALID_REQUEST = "invalid_request"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    CANCELLED = "cancelled"
    INTERNAL = "internal"


# Single mapping table used for errors that occur BEFORE the first byte of a stream.
HTTP_STATUS = {
    ErrorClass.AUTH: 502,             # our credentials to the provider failed: not the caller's fault
    ErrorClass.RATE_LIMIT: 429,
    ErrorClass.INVALID_REQUEST: 400,
    ErrorClass.UNAVAILABLE: 503,
    ErrorClass.TIMEOUT: 504,
    ErrorClass.PROTOCOL: 502,
    ErrorClass.CANCELLED: 499,
    ErrorClass.INTERNAL: 500,
}

DEFAULT_RETRYABLE = {
    ErrorClass.RATE_LIMIT: True,
    ErrorClass.UNAVAILABLE: True,
    ErrorClass.TIMEOUT: True,
}

PUBLIC_MESSAGES = {
    ErrorClass.AUTH: "The AI service is misconfigured. Please try again later.",
    ErrorClass.RATE_LIMIT: "The AI service is busy. Please retry shortly.",
    ErrorClass.INVALID_REQUEST: "The request could not be processed.",
    ErrorClass.UNAVAILABLE: "The AI service is temporarily unavailable.",
    ErrorClass.TIMEOUT: "The AI service took too long to respond.",
    ErrorClass.PROTOCOL: "The AI service returned an unreadable response.",
    ErrorClass.CANCELLED: "The request was cancelled.",
    ErrorClass.INTERNAL: "An internal error occurred.",
}


@dataclass(frozen=True)
class InferenceError:
    cls: ErrorClass
    retryable: bool = False
    retry_after_s: Optional[float] = None
    public_message: str = ""
    detail: str = ""  # logs only; never serialised to clients

    @staticmethod
    def of(cls: ErrorClass, retry_after_s: Optional[float] = None, detail: str = "") -> "InferenceError":
        return InferenceError(cls, DEFAULT_RETRYABLE.get(cls, False), retry_after_s, PUBLIC_MESSAGES[cls], detail)


def classify_http_status(status: int) -> ErrorClass:
    """Generic HTTP-status classifier an adapter may use as a starting point."""
    if status in (401, 403):
        return ErrorClass.AUTH
    if status == 429:
        return ErrorClass.RATE_LIMIT
    if status in (408, 504):
        return ErrorClass.TIMEOUT
    if 400 <= status < 500:
        return ErrorClass.INVALID_REQUEST
    if status in (500, 502, 503, 529):
        return ErrorClass.UNAVAILABLE
    return ErrorClass.INTERNAL
