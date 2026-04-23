from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from warcio.archiveiterator import ArchiveIterator


def print_progress(processed_files: int, total_files: int, documents: int) -> None:
    if total_files <= 0:
        return
    pct = int((processed_files * 100) / total_files)
    bar_slots = 20
    filled = int((pct / 100) * bar_slots)
    bar = "#" * filled + "-" * (bar_slots - filled)
    sys.stderr.write(
        f"\r[corpus_stats] [{bar}] {pct}% ({processed_files}/{total_files} files, {documents} docs)"
    )
    sys.stderr.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute basic corpus statistics from WARC files")
    parser.add_argument("path", nargs="?", default="corpus", help="Directory containing .warc.gz files")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    corpus_dir = Path(args.path)
    warc_files = sorted(corpus_dir.glob("*.warc.gz"))

    domain_counts: Counter[str] = Counter()
    domains: set[str] = set()

    total_files = len(warc_files)
    for idx, warc_file in enumerate(warc_files, start=1):
        with gzip.open(warc_file, "rb") as handle:
            for record in ArchiveIterator(handle):
                if record.rec_type != "response":
                    continue

                target_uri = record.rec_headers.get_header("WARC-Target-URI")
                if not target_uri:
                    continue

                payload = record.content_stream().read()
                if not payload:
                    continue

                domain = urlsplit(target_uri).netloc.lower()
                domains.add(domain)
                domain_counts[domain] += 1
        print_progress(idx, total_files, sum(domain_counts.values()))

    if total_files > 0:
        sys.stderr.write("\n")
        sys.stderr.flush()

    result = {
        "warc_files": len(warc_files),
        "documents": sum(domain_counts.values()),
        "unique_domains": len(domains),
        "pages_per_domain": dict(domain_counts.most_common()),
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"WARC files: {result['warc_files']}")
        print(f"Documents: {result['documents']}")
        print(f"Unique domains: {result['unique_domains']}")
        print("Pages per domain:")
        for domain, count in domain_counts.most_common(20):
            print(f"  {domain}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())