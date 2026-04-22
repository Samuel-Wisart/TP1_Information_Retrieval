from __future__ import annotations

import threading
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import requests
from protego import Protego


@dataclass(frozen=True)
class _RobotsPolicy:
    parser: Protego
    crawl_delay: float


class RobotsCache:
    def __init__(self, user_agent: str = "UFMG-IR-PA1-Crawler") -> None:
        self.user_agent = user_agent
        self._lock = threading.Lock()
        self._policies: dict[str, _RobotsPolicy] = {}

    def get_policy(self, url: str, throttle=None) -> tuple[_RobotsPolicy, bool]:
        parts = urlsplit(url)
        host_key = f"{parts.scheme}://{parts.netloc}".lower()

        with self._lock:
            cached = self._policies.get(host_key)
        if cached is not None:
            return cached, False

        if throttle is not None:
            throttle(parts.netloc.lower(), 0.1)

        robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
        crawl_delay = 0.1
        parser = Protego.parse("")

        try:
            response = requests.get(
                robots_url,
                headers={"User-Agent": self.user_agent, "Accept": "text/plain,*/*;q=0.1"},
                timeout=10,
            )
            if response.ok:
                parser = Protego.parse(response.text)
            parsed_delay = parser.crawl_delay(self.user_agent)
            if parsed_delay is not None:
                crawl_delay = max(crawl_delay, float(parsed_delay))
        except (requests.RequestException, ValueError):
            parser = Protego.parse("")

        policy = _RobotsPolicy(parser=parser, crawl_delay=crawl_delay)
        with self._lock:
            self._policies[host_key] = policy
        return policy, True