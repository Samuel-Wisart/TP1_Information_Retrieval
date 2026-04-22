from __future__ import annotations

import warnings

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from crawlerlib.url_tools import normalize_url

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


def extract_html_data(html_bytes: bytes, base_url: str) -> tuple[str, str, list[str]]:
    soup = BeautifulSoup(html_bytes, "html.parser")

    for node in soup(["script", "style", "noscript"]):
        node.decompose()

    title_tag = soup.title.string if soup.title and soup.title.string else ""
    title = " ".join(title_tag.split())
    text = " ".join(soup.get_text(" ", strip=True).split())

    resolved_links: list[str] = []
    for tag in soup.find_all("a", href=True):
        normalized = normalize_url(tag.get("href"), base=base_url)
        if normalized is not None and normalized not in resolved_links:
            resolved_links.append(normalized)
    return title, text, resolved_links