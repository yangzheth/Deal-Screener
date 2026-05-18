from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; ClarityActMonitor/0.1)"


class HttpJsonError(RuntimeError):
    """Raised when a JSON HTTP request fails or returns a non-2xx status."""


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: Any | None = None,
    timeout: int = 30,
) -> Any:
    """Perform an HTTP request and decode a JSON response.

    Kept stdlib-only to match the rest of the repository. Network and decode
    failures are normalized into HttpJsonError so callers can degrade cleanly.
    """
    request_headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        request_headers.update(headers)

    data: bytes | None = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = Request(url, data=data, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HttpJsonError(f"{method} {url} failed: HTTP {exc.code} {detail}") from exc
    except Exception as exc:  # pragma: no cover - defensive for remote feeds
        raise HttpJsonError(f"{method} {url} failed: {exc}") from exc

    if not body:
        return {}
    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HttpJsonError(f"{method} {url} returned invalid JSON: {exc}") from exc


def get_json(url: str, *, headers: dict[str, str] | None = None, timeout: int = 30) -> Any:
    return request_json("GET", url, headers=headers, timeout=timeout)
