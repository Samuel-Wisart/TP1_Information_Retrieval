from __future__ import annotations

import argparse
import json
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from crawlerlib.html_tools import extract_html_data
from crawlerlib.robots import RobotsCache
from crawlerlib.url_tools import get_host, is_html_content_type, normalize_url
from crawlerlib.warc import WarcWriter


class CrawlManager:
    def __init__(self, seeds: list[str], limit: int, debug: bool, output_dir: Path):
        self.limit = limit
        self.debug = debug
        self.output_dir = output_dir
        self.frontier: queue.Queue[str] = queue.Queue()
        self.seen: set[str] = set()
        self.seen_lock = threading.Lock()
        self.count_lock = threading.Lock()
        self.print_lock = threading.Lock()
        self.stored_count = 0
        self.stop_event = threading.Event()
        self.robots = RobotsCache()
        self.writer = WarcWriter(output_dir)
        self.rate_limit_lock = threading.Lock()
        self.next_allowed_by_host: dict[str, float] = {}
        self.minimum_delay = 0.1

        for seed in seeds:
            normalized = normalize_url(seed)
            if normalized is None:
                continue
            with self.seen_lock:
                if normalized in self.seen:
                    continue
                self.seen.add(normalized)
            self.frontier.put(normalized)

    def close(self) -> None:
        self.writer.close()

    def crawl(self, workers: int) -> None:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(self._worker) for _ in range(workers)]
            self.frontier.join()
            self.stop_event.set()
            for future in futures:
                future.result()

    def _worker(self) -> None:
        while True:
            try:
                url = self.frontier.get(timeout=0.5)
            except queue.Empty:
                if self.stop_event.is_set():
                    return
                continue

            try:
                if self.stop_event.is_set():
                    continue
                try:
                    self._process_url(url)
                except Exception:
                    continue
            finally:
                self.frontier.task_done()

    def _process_url(self, url: str) -> None:
        host = get_host(url)
        if host is None:
            return

        policy, _ = self.robots.get_policy(url, throttle=self._acquire_host_slot)
        if not policy.parser.can_fetch(self.robots.user_agent, url):
            return

        page_delay = max(self.minimum_delay, policy.crawl_delay)
        self._acquire_host_slot(host, page_delay)

        fetched_at = int(time.time())
        response = self._fetch(url)
        if response is None:
            return

        content_type = response.headers.get("Content-Type", "")
        if not is_html_content_type(content_type):
            return

        body = response.read()
        final_url = normalize_url(response.geturl()) or url
        title, text, outlinks = extract_html_data(body, final_url)

        if not self._reserve_slot(final_url):
            return

        self.writer.write(
            url=final_url,
            body=body,
            status=getattr(response, "status", 200),
            reason=getattr(response, "reason", "OK"),
            headers=list(response.headers.items()),
            timestamp=fetched_at,
        )
        if self.debug:
            self._emit_debug_record(final_url, title, text, fetched_at)

        for outlink in outlinks:
            normalized = normalize_url(outlink, base=final_url)
            if normalized is None:
                continue
            with self.seen_lock:
                if normalized in self.seen or self.stop_event.is_set():
                    continue
                self.seen.add(normalized)
            self.frontier.put(normalized)

    def _fetch(self, url: str):
        import urllib.error
        import urllib.request

        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": self.robots.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "identity",
            },
        )
        try:
            return urllib.request.urlopen(request, timeout=20)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError):
            return None

    def _reserve_slot(self, url: str) -> bool:
        with self.count_lock:
            if self.stored_count >= self.limit:
                self.stop_event.set()
                return False
            self.stored_count += 1
            if self.stored_count >= self.limit:
                self.stop_event.set()
            return True

    def _emit_debug_record(self, url: str, title: str, text: str, timestamp: int) -> None:
        words = text.split()
        payload = {
            "URL": url,
            "Title": title,
            "Text": " ".join(words[:20]),
            "Timestamp": timestamp,
        }
        with self.print_lock:
            print(json.dumps(payload, ensure_ascii=False), flush=True)

    def _acquire_host_slot(self, host: str, delay: float) -> None:
        while True:
            with self.rate_limit_lock:
                now = time.monotonic()
                ready_at = self.next_allowed_by_host.get(host, 0.0)
                if now >= ready_at:
                    self.next_allowed_by_host[host] = now + delay
                    return
                sleep_for = ready_at - now
            time.sleep(sleep_for)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polite multi-threaded web crawler")
    parser.add_argument("-s", required=True, help="Path to the seed URL file")
    parser.add_argument("-n", required=True, type=int, help="Target number of webpages to crawl")
    parser.add_argument("-d", action="store_true", help="Enable debug output")
    return parser.parse_args()


def load_seeds(path: str) -> list[str]:
    seeds: list[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            seed = line.strip()
            if not seed or seed.startswith("#"):
                continue
            seeds.append(seed)
    return seeds


def main() -> int:
    args = parse_args()
    if args.n <= 0:
        raise SystemExit("-n must be a positive integer")

    seeds = load_seeds(args.s)
    if not seeds:
        raise SystemExit("No valid seeds found")

    output_dir = Path("corpus")
    manager = CrawlManager(seeds=seeds, limit=args.n, debug=args.d, output_dir=output_dir)
    try:
        manager.crawl(workers=24)
    finally:
        manager.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())