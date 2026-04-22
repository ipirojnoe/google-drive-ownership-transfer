import random
import socket
import ssl
import time
from collections.abc import Callable
from typing import TypeVar

import httplib2
import requests
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError

from src.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

DEFAULT_ATTEMPTS = 8
DEFAULT_BASE_DELAY = 1.0
DEFAULT_MAX_DELAY = 60.0

TRANSIENT_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}
TRANSIENT_HTTP_REASONS = {
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "userRateLimitExceeded",
}


def _http_error_reason(exc: HttpError) -> str | None:
    try:
        payload = exc.error_details
    except AttributeError:
        return None

    if not payload:
        return None

    first = payload[0]
    if not isinstance(first, dict):
        return None
    return first.get("reason")


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, HttpError):
        status = getattr(exc.resp, "status", None)
        reason = _http_error_reason(exc)
        return status in TRANSIENT_HTTP_STATUSES or reason in TRANSIENT_HTTP_REASONS

    return isinstance(
        exc,
        (
            TransportError,
            httplib2.ServerNotFoundError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
            socket.timeout,
            socket.gaierror,
            ssl.SSLError,
        ),
    )


def retry_transient(
    operation: str,
    func: Callable[[], T],
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
) -> T:
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt >= attempts or not is_transient_error(exc):
                raise

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = delay * random.uniform(0.75, 1.25)
            logger.warning(
                "%s failed with a transient error (%s). Retrying in %.1fs (%d/%d)...",
                operation,
                exc,
                delay,
                attempt,
                attempts,
            )
            time.sleep(delay)

    raise RuntimeError(f"Retry loop for {operation} exited unexpectedly")
