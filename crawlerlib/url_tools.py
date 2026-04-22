from __future__ import annotations

from urllib.parse import urljoin, urlsplit

from url_normalize import url_normalize


def normalize_url(url: str, base: str | None = None) -> str | None:
    candidate = urljoin(base, url) if base else url
    candidate = candidate.strip()
    if not candidate:
        return None

    try:
        normalized = url_normalize(candidate)
    except ValueError:
        return None

    parts = urlsplit(normalized)
    if parts.scheme.lower() not in {"http", "https"}:
        return None
    if not parts.netloc:
        return None
    return normalized


def get_host(url: str) -> str | None:
    parts = urlsplit(url)
    if not parts.netloc:
        return None
    return parts.netloc.lower()


def is_html_content_type(content_type: str) -> bool:
    content_type = content_type.lower()
    return content_type.startswith("text/html") or "application/xhtml+xml" in content_type