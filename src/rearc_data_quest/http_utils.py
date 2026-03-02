from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class HttpResponse:
    def __init__(self, *, url: str, status_code: int, content: bytes) -> None:
        self.url = url
        self.status_code = status_code
        self.content = content

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code} for URL: {self.url}")


class HttpSession:
    def __init__(self, user_agent: str | None = None) -> None:
        self.headers: dict[str, str] = {}
        if user_agent:
            self.headers["User-Agent"] = user_agent

    def get(self, url: str, *, timeout: int = 30) -> HttpResponse:
        req = Request(url=url, headers=self.headers, method="GET")
        try:
            with urlopen(req, timeout=timeout) as resp:
                return HttpResponse(
                    url=url,
                    status_code=getattr(resp, "status", resp.getcode()),
                    content=resp.read(),
                )
        except HTTPError as exc:
            return HttpResponse(
                url=url,
                status_code=exc.code,
                content=exc.read() if hasattr(exc, "read") else b"",
            )
        except URLError as exc:
            raise RuntimeError(f"Network error for URL {url}: {exc}") from exc

    def close(self) -> None:
        return None

    def __enter__(self) -> HttpSession:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def build_session(user_agent: str | None = None) -> HttpSession:
    return HttpSession(user_agent)


def get_json(url: str, *, session: HttpSession | None = None, timeout: int = 30) -> dict[str, Any]:
    owned_session = session or HttpSession()
    response = owned_session.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload
