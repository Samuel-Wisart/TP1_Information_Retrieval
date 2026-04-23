from __future__ import annotations

import argparse
import gzip
import sys
import zlib
from pathlib import Path

from warcio.archiveiterator import ArchiveIterator

from crawlerlib.html_tools import extract_html_data


def print_progress(processed_files: int, total_files: int, total_docs: int) -> None:
    if total_files <= 0:
        return
    pct = int((processed_files * 100) / total_files)
    bar_slots = 20
    filled = int((pct / 100) * bar_slots)
    bar = "#" * filled + "-" * (bar_slots - filled)
    sys.stderr.write(
        f"\r[token_stats] [{bar}] {pct}% ({processed_files}/{total_files} files, {total_docs} docs)"
    )
    sys.stderr.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute token statistics per webpage from WARC files")
    parser.add_argument("path", nargs="?", default="corpus", help="Directory containing .warc.gz files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    corpus_dir = Path(args.path)
    warc_files = sorted(corpus_dir.glob("*.warc.gz"))

    total_docs = 0
    total_tokens = 0
    max_tokens = 0

    total_files = len(warc_files)
    for idx, warc_file in enumerate(warc_files, start=1):
        try:
            with gzip.open(warc_file, "rb") as handle:
                for record in ArchiveIterator(handle):
                    if record.rec_type != "response":
                        continue

                    target_uri = record.rec_headers.get_header("WARC-Target-URI")
                    if not target_uri:
                        continue

                    try:
                        payload = record.content_stream().read()
                    except (EOFError, OSError, zlib.error):
                        continue

                    if not payload:
                        continue

                    _, visible_text, _ = extract_html_data(payload, target_uri)
                    token_count = len(visible_text.split())

                    total_docs += 1
                    total_tokens += token_count
                    if token_count > max_tokens:
                        max_tokens = token_count
        except (EOFError, OSError, zlib.error):
            continue

        avg_tokens = (total_tokens / total_docs) if total_docs else 0.0
        print_progress(idx, total_files, total_docs)
        sys.stderr.write(f" | avg_tokens={avg_tokens:.2f} max_tokens={max_tokens}")
        sys.stderr.flush()

    if total_files > 0:
        sys.stderr.write("\n")
        sys.stderr.flush()

    avg_tokens = (total_tokens / total_docs) if total_docs else 0.0

    print(f"documents: {total_docs}")
    print(f"avg_tokens_per_page: {avg_tokens:.2f}")
    print(f"max_tokens_per_page: {max_tokens}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())