import pytest
from bbp_reliability import ErrorClass, HTTP_STATUS, InferenceError, classify_http_status


def test_every_error_class_has_an_http_status():
    assert set(HTTP_STATUS) == set(ErrorClass)  # exhaustive mapping


@pytest.mark.parametrize("status,expected", [
    (401, ErrorClass.AUTH), (403, ErrorClass.AUTH), (429, ErrorClass.RATE_LIMIT),
    (400, ErrorClass.INVALID_REQUEST), (503, ErrorClass.UNAVAILABLE), (504, ErrorClass.TIMEOUT),
])
def test_status_classification(status, expected):
    assert classify_http_status(status) == expected


def test_retryability_defaults():
    assert InferenceError.of(ErrorClass.RATE_LIMIT).retryable
    assert InferenceError.of(ErrorClass.UNAVAILABLE).retryable
    assert not InferenceError.of(ErrorClass.AUTH).retryable
    assert not InferenceError.of(ErrorClass.INVALID_REQUEST).retryable
