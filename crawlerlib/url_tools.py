from __future__ import annotations

from urllib.parse import urljoin, urlsplit, urlunsplit


def normalize_url(url: str, base: str | None = None) -> str | None:
    candidate = urljoin(base, url) if base else url
    candidate = candidate.strip()
    if not candidate:
        return None

    parts = urlsplit(candidate)
    if parts.scheme.lower() not in {"http", "https"}:
        return None
    if not parts.netloc:
        return None

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = parts.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    query = parts.query
    return urlunsplit((scheme, netloc, path, query, ""))


def get_host(url: str) -> str | None:
    parts = urlsplit(url)
    if not parts.netloc:
        return None
    return parts.netloc.lower()


def is_html_content_type(content_type: str) -> bool:
    content_type = content_type.lower()
    return content_type.startswith("text/html") or "application/xhtml+xml" in content_type