from __future__ import annotations

import gzip
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


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

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            if self._raw_handle is not None:
                self._raw_handle.close()
                self._raw_handle = None

    def write(
        self,
        url: str,
        body: bytes,
        status: int,
        reason: str,
        headers: list[tuple[str, str]],
        timestamp: int,
    ) -> None:
        http_payload = self._build_http_payload(status=status, reason=reason, headers=headers, body=body)
        warc_block = self._build_warc_block(url=url, payload=http_payload, timestamp=timestamp)

        with self._lock:
            if self._handle is None or self._record_count >= self.records_per_file:
                self._rotate_file()
            self._handle.write(warc_block)
            self._record_count += 1

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

    def _build_http_payload(self, status: int, reason: str, headers: list[tuple[str, str]], body: bytes) -> bytes:
        status_line = f"HTTP/1.1 {status} {reason}\r\n"
        header_lines = [f"{name}: {value}" for name, value in headers]
        return (status_line + "\r\n".join(header_lines) + "\r\n\r\n").encode("utf-8", errors="ignore") + body

    def _build_warc_block(self, url: str, payload: bytes, timestamp: int) -> bytes:
        warc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        record_id = f"<urn:uuid:{uuid.uuid4()}>"
        headers = [
            "WARC/1.0",
            "WARC-Type: response",
            f"WARC-Record-ID: {record_id}",
            f"WARC-Target-URI: {url}",
            f"WARC-Date: {warc_date}",
            "Content-Type: application/http; msgtype=response",
            f"Content-Length: {len(payload)}",
            "",
            "",
        ]
        header_bytes = "\r\n".join(headers).encode("utf-8")
        return header_bytes + payload + b"\r\n\r\n"