from __future__ import annotations

import threading
from io import BytesIO
from pathlib import Path
import re

import gzip
from warcio.archiveiterator import ArchiveIterator

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


class WarcWriter:
    def __init__(self, base_dir: Path, records_per_file: int = 1000) -> None:
        self.base_dir = base_dir
        self.records_per_file = records_per_file
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._record_count = 0
        self._file_index = 0
        self._handle = None
        self._raw_handle = None
        self._writer = None
        self._resume_from_existing_files()

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            if self._raw_handle is not None:
                self._raw_handle.close()
                self._raw_handle = None
            self._writer = None

    def write(
        self,
        url: str,
        body: bytes,
        status: int,
        reason: str,
        headers: list[tuple[str, str]],
        timestamp: int,
    ) -> tuple[int, int, int]:
        with self._lock:
            if self._handle is None or self._record_count >= self.records_per_file:
                self._rotate_file()
            assert self._writer is not None
            record_headers = StatusAndHeaders(
                f"{status} {reason}",
                headers,
                protocol="HTTP/1.1",
            )
            record = self._writer.create_warc_record(
                url,
                "response",
                payload=BytesIO(body),
                http_headers=record_headers,
                warc_headers_dict={"WARC-Date": self._format_warc_date(timestamp)},
            )
            self._writer.write_record(record)
            self._record_count += 1
            return self._file_index, self._record_count, self.records_per_file

    def get_position(self) -> tuple[int, int, int]:
        with self._lock:
            return self._file_index, self._record_count, self.records_per_file

    def _rotate_file(self) -> None:
        if self._handle is not None:
            self._handle.close()
        if self._raw_handle is not None:
            self._raw_handle.close()
        self._file_index += 1
        self._record_count = 0
        file_path = self.base_dir / f"warc-{self._file_index:05d}.warc.gz"
        self._raw_handle = file_path.open("wb")
        self._handle = gzip.GzipFile(fileobj=self._raw_handle, mode="wb")
        self._writer = WARCWriter(self._handle, gzip=False)

    def _resume_from_existing_files(self) -> None:
        existing = sorted(self.base_dir.glob("warc-*.warc.gz"))
        if not existing:
            return

        last_file = existing[-1]
        last_index = self._parse_index(last_file.name)
        if last_index is None:
            return

        self._file_index = last_index
        self._record_count = self._count_response_records(last_file)
        
        try:
            self._raw_handle = last_file.open("ab")
            self._handle = gzip.GzipFile(fileobj=self._raw_handle, mode="ab")
            self._writer = WARCWriter(self._handle, gzip=False)
        except (OSError, EOFError):
            self._rotate_file()

    def _parse_index(self, name: str) -> int | None:
        match = re.fullmatch(r"warc-(\d+)\.warc\.gz", name)
        if match is None:
            return None
        return int(match.group(1))

    def _count_response_records(self, warc_file: Path) -> int:
        count = 0
        try:
            with gzip.open(warc_file, "rb") as handle:
                for record in ArchiveIterator(handle):
                    if record.rec_type == "response":
                        count += 1
        except (EOFError, OSError):
            return 0
        return count

    def _format_warc_date(self, timestamp: int) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")