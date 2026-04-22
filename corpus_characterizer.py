from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
import warnings
import zlib
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from warcio.archiveiterator import ArchiveIterator

from crawlerlib.html_tools import extract_html_data

warnings.filterwarnings(
    "ignore",
    message=r"Some characters could not be decoded, and were replaced with REPLACEMENT CHARACTER\.",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Characterize a crawled corpus stored as .warc.gz files")
    parser.add_argument("path", nargs="?", default="corpus", help="Directory containing WARC gzip files")
    parser.add_argument("--json", action="store_true", help="Print full JSON output")
    parser.add_argument("--top-domains", type=int, default=25, help="Number of top domains to show in text mode")
    parser.add_argument("--token-bucket-size", type=int, default=100, help="Token bucket width for histogram")
    parser.add_argument("--max-docs", type=int, default=0, help="Optional cap for analyzed documents (0 means all)")
    parser.add_argument("--output", default="", help="Optional output file path to save the report")
    return parser.parse_args()


def percentile(sorted_values: list[int], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return float(sorted_values[0])
    if p >= 100:
        return float(sorted_values[-1])

    pos = (len(sorted_values) - 1) * (p / 100.0)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def bucketize(values: list[int], bucket_size: int) -> dict[str, int]:
    hist = Counter()
    width = max(1, bucket_size)
    for value in values:
        start = (value // width) * width
        end = start + width - 1
        hist[f"{start}-{end}"] += 1
    return dict(sorted(hist.items()))


def domain_size_distribution(domain_counts: Counter[str]) -> dict[str, int]:
    dist = Counter()
    for pages in domain_counts.values():
        if pages == 1:
            dist["1"] += 1
        elif 2 <= pages <= 5:
            dist["2-5"] += 1
        elif 6 <= pages <= 10:
            dist["6-10"] += 1
        elif 11 <= pages <= 50:
            dist["11-50"] += 1
        elif 51 <= pages <= 100:
            dist["51-100"] += 1
        else:
            dist["101+"] += 1
    return dict(dist)


def safe_status_code(record) -> str:
    if not record.http_headers:
        return "unknown"
    status_line = record.http_headers.get_statuscode()
    return status_line or "unknown"


def safe_content_type(record) -> str:
    if not record.http_headers:
        return "unknown"
    value = record.http_headers.get_header("Content-Type")
    if not value:
        return "unknown"
    return value.split(";", 1)[0].strip().lower() or "unknown"


def summarize(values: list[int]) -> dict[str, float]:
    if not values:
        return {"min": 0, "max": 0, "avg": 0.0, "median": 0.0, "p90": 0.0, "p95": 0.0}

    sorted_values = sorted(values)
    return {
        "min": int(sorted_values[0]),
        "max": int(sorted_values[-1]),
        "avg": float(sum(sorted_values) / len(sorted_values)),
        "median": float(statistics.median(sorted_values)),
        "p90": float(percentile(sorted_values, 90)),
        "p95": float(percentile(sorted_values, 95)),
    }


def characterize(corpus_dir: Path, token_bucket_size: int, max_docs: int = 0) -> dict:
    warc_files = sorted(corpus_dir.glob("*.warc.gz"))

    domain_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    content_type_counts: Counter[str] = Counter()
    token_counts: list[int] = []
    html_sizes: list[int] = []
    outlink_counts: list[int] = []
    titles_missing = 0
    total_docs = 0
    skipped_files: list[str] = []
    skipped_records = 0

    for warc_file in warc_files:
        try:
            with gzip.open(warc_file, "rb") as handle:
                for record in ArchiveIterator(handle):
                    if max_docs > 0 and total_docs >= max_docs:
                        break
                    if record.rec_type != "response":
                        continue

                    target_uri = record.rec_headers.get_header("WARC-Target-URI")
                    if not target_uri:
                        continue

                    try:
                        payload = record.content_stream().read()
                    except (zlib.error, EOFError, OSError):
                        skipped_records += 1
                        continue

                    if not payload:
                        continue

                    total_docs += 1
                    domain = urlsplit(target_uri).netloc.lower()
                    if domain:
                        domain_counts[domain] += 1

                    status_counts[safe_status_code(record)] += 1
                    content_type_counts[safe_content_type(record)] += 1

                    html_sizes.append(len(payload))
                    title, text, outlinks = extract_html_data(payload, target_uri)
                    token_counts.append(len(text.split()))
                    outlink_counts.append(len(outlinks))
                    if not title:
                        titles_missing += 1
        except (zlib.error, EOFError, OSError):
            skipped_files.append(warc_file.name)
        if max_docs > 0 and total_docs >= max_docs:
            break

    result = {
        "required": {
            "documents": total_docs,
            "unique_domains": len(domain_counts),
            "pages_per_domain": dict(domain_counts.most_common()),
            "domain_size_distribution": domain_size_distribution(domain_counts),
            "tokens_per_webpage_distribution": bucketize(token_counts, token_bucket_size),
        },
        "extra": {
            "warc_files": len(warc_files),
            "analyzed_documents": total_docs,
            "skipped_corrupted_files": skipped_files,
            "skipped_corrupted_record_count": skipped_records,
            "status_code_distribution": dict(status_counts.most_common()),
            "content_type_distribution": dict(content_type_counts.most_common()),
            "html_size_bytes": summarize(html_sizes),
            "tokens_per_webpage": summarize(token_counts),
            "outlinks_per_webpage": summarize(outlink_counts),
            "pages_without_title": titles_missing,
            "pages_without_title_ratio": (titles_missing / total_docs) if total_docs else 0.0,
        },
    }
    return result


def build_text_report(data: dict, top_domains: int) -> str:
    req = data["required"]
    extra = data["extra"]
    lines: list[str] = []

    lines.append("=== Required Statistics ===")
    lines.append(f"Documents: {req['documents']}")
    lines.append(f"Unique domains: {req['unique_domains']}")
    lines.append("Top domains by page count:")
    for domain, pages in list(req["pages_per_domain"].items())[:top_domains]:
        lines.append(f"  {domain}: {pages}")

    lines.append("Domain size distribution (number of domains by number of pages):")
    for bucket, count in req["domain_size_distribution"].items():
        lines.append(f"  {bucket}: {count}")

    lines.append("Token distribution per webpage (bucketed):")
    for bucket, count in req["tokens_per_webpage_distribution"].items():
        lines.append(f"  {bucket}: {count}")

    lines.append("")
    lines.append("=== Extra Statistics ===")
    lines.append(f"WARC files: {extra['warc_files']}")
    lines.append(f"Skipped corrupted files: {len(extra['skipped_corrupted_files'])}")
    lines.append(f"Skipped corrupted records: {extra['skipped_corrupted_record_count']}")

    lines.append("Status code distribution:")
    for status, count in extra["status_code_distribution"].items():
        lines.append(f"  {status}: {count}")

    lines.append("Content-Type distribution:")
    for ctype, count in extra["content_type_distribution"].items():
        lines.append(f"  {ctype}: {count}")

    html_size = extra["html_size_bytes"]
    lines.append(
        "HTML size bytes (min/avg/median/p90/p95/max): "
        f"{html_size['min']} / {html_size['avg']:.2f} / {html_size['median']:.2f} / "
        f"{html_size['p90']:.2f} / {html_size['p95']:.2f} / {html_size['max']}"
    )

    token_size = extra["tokens_per_webpage"]
    lines.append(
        "Tokens per webpage (min/avg/median/p90/p95/max): "
        f"{token_size['min']} / {token_size['avg']:.2f} / {token_size['median']:.2f} / "
        f"{token_size['p90']:.2f} / {token_size['p95']:.2f} / {token_size['max']}"
    )

    outlinks = extra["outlinks_per_webpage"]
    lines.append(
        "Outlinks per webpage (min/avg/median/p90/p95/max): "
        f"{outlinks['min']} / {outlinks['avg']:.2f} / {outlinks['median']:.2f} / "
        f"{outlinks['p90']:.2f} / {outlinks['p95']:.2f} / {outlinks['max']}"
    )

    ratio = extra["pages_without_title_ratio"] * 100.0
    lines.append(f"Pages without title: {extra['pages_without_title']} ({ratio:.2f}%)")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    corpus_dir = Path(args.path)
    data = characterize(corpus_dir=corpus_dir, token_bucket_size=args.token_bucket_size, max_docs=max(0, args.max_docs))

    output_text = ""
    if args.json:
        output_text = json.dumps(data, ensure_ascii=False, indent=2)
    else:
        output_text = build_text_report(data, top_domains=max(1, args.top_domains))

    print(output_text)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        print(f"\nSaved report to: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
