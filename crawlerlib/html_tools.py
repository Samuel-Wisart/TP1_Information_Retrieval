from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser

from crawlerlib.url_tools import normalize_url


class _HtmlExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._title_buffer: list[str] = []
        self._text_buffer: list[str] = []
        self._link_buffer: list[str] = []
        self._capture_title = False
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs):
        if tag in {"script", "style", "noscript"}:
            self._ignored_depth += 1
            return
        if tag == "title":
            self._capture_title = True
            return
        if tag == "a" and self._ignored_depth == 0:
            href = dict(attrs).get("href")
            if href:
                self._link_buffer.append(href)

    def handle_endtag(self, tag: str):
        if tag in {"script", "style", "noscript"} and self._ignored_depth > 0:
            self._ignored_depth -= 1
            return
        if tag == "title":
            self._capture_title = False

    def handle_data(self, data: str):
        if self._ignored_depth > 0:
            return
        if self._capture_title:
            self._title_buffer.append(data)
        else:
            self._text_buffer.append(data)

    def result(self) -> tuple[str, str, list[str]]:
        title = _normalize_text("".join(self._title_buffer))
        text = _normalize_text("".join(self._text_buffer))
        links = [link for link in self._link_buffer if link]
        return title, text, links


def _normalize_text(text: str) -> str:
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_html_data(html_bytes: bytes, base_url: str) -> tuple[str, str, list[str]]:
    extractor = _HtmlExtractor()
    try:
        extractor.feed(html_bytes.decode("utf-8", errors="ignore"))
    finally:
        extractor.close()
    title, text, links = extractor.result()

    resolved_links: list[str] = []
    for link in links:
        normalized = normalize_url(link, base=base_url)
        if normalized is not None:
            resolved_links.append(normalized)
    return title, text, resolved_links