from __future__ import annotations

import argparse
import gzip
import zlib
from pathlib import Path

from warcio.archiveiterator import ArchiveIterator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Count response documents per .warc.gz file")
    parser.add_argument("path", nargs="?", default="corpus", help="Directory containing .warc.gz files")
    return parser.parse_args()


def count_docs_in_warc(warc_file: Path) -> int:
    count = 0
    try:
        with gzip.open(warc_file, "rb") as handle:
            for record in ArchiveIterator(handle):
                if record.rec_type == "response":
                    count += 1
    except (EOFError, OSError, zlib.error):
        pass
    return count


def main() -> int:
    args = parse_args()
    corpus_dir = Path(args.path)
    warc_files = sorted(corpus_dir.glob("*.warc.gz"))

    for warc_file in warc_files:
        doc_count = count_docs_in_warc(warc_file)
        print(f"{warc_file.name}: {doc_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())