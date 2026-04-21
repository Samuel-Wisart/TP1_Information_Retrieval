from __future__ import annotations

import argparse
import gzip
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit


TOKEN_PATTERN = re.compile(r"\b\w+\b", re.UNICODE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute basic corpus statistics from WARC files")
    parser.add_argument("path", nargs="?", default="corpus", help="Directory containing .warc.gz files")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    return parser.parse_args()


def iter_warc_records(file_path: Path):
    with gzip.open(file_path, "rb") as handle:
        while True:
            line = handle.readline()
            if not line:
                break
            if line != b"WARC/1.0\r\n":
                continue

            headers: dict[str, str] = {}
            while True:
                header_line = handle.readline()
                if not header_line or header_line == b"\r\n":
                    break
                decoded = header_line.decode("utf-8", errors="ignore").rstrip("\r\n")
                if ": " in decoded:
                    name, value = decoded.split(": ", 1)
                    headers[name.lower()] = value

            content_length = int(headers.get("content-length", "0"))
            payload = handle.read(content_length)
            handle.readline()
            handle.readline()
            yield headers, payload


def extract_http_body(payload: bytes) -> bytes:
    separator = payload.find(b"\r\n\r\n")
    if separator == -1:
        return payload
    return payload[separator + 4 :]


def count_tokens(html_bytes: bytes) -> int:
    text = html_bytes.decode("utf-8", errors="ignore")
    return len(TOKEN_PATTERN.findall(text))


def main() -> int:
    args = parse_args()
    corpus_dir = Path(args.path)
    warc_files = sorted(corpus_dir.glob("*.warc.gz"))

    domain_counts: Counter[str] = Counter()
    token_counts: list[int] = []
    domains: set[str] = set()

    for warc_file in warc_files:
        for headers, payload in iter_warc_records(warc_file):
            target_uri = headers.get("warc-target-uri")
            if not target_uri:
                continue
            domain = urlsplit(target_uri).netloc.lower()
            domains.add(domain)
            domain_counts[domain] += 1
            token_counts.append(count_tokens(extract_http_body(payload)))

    token_distribution = Counter()
    for count in token_counts:
        bucket = f"{(count // 100) * 100}-{(count // 100) * 100 + 99}"
        token_distribution[bucket] += 1

    result = {
        "warc_files": len(warc_files),
        "documents": sum(domain_counts.values()),
        "unique_domains": len(domains),
        "pages_per_domain": dict(domain_counts.most_common()),
        "token_count_buckets": dict(sorted(token_distribution.items())),
        "token_count_min": min(token_counts) if token_counts else 0,
        "token_count_max": max(token_counts) if token_counts else 0,
        "token_count_avg": (sum(token_counts) / len(token_counts)) if token_counts else 0.0,
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"WARC files: {result['warc_files']}")
        print(f"Documents: {result['documents']}")
        print(f"Unique domains: {result['unique_domains']}")
        print(f"Token count min/avg/max: {result['token_count_min']} / {result['token_count_avg']:.2f} / {result['token_count_max']}")
        print("Pages per domain:")
        for domain, count in domain_counts.most_common(20):
            print(f"  {domain}: {count}")
        print("Token buckets:")
        for bucket, count in sorted(token_distribution.items()):
            print(f"  {bucket}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())