"""Shared, dependency-free helpers for the Pump.fun MVP.

The package deliberately uses the Python standard library so a clean Colab or
local virtual environment can run it without private caches or credentials.
"""

from __future__ import annotations

import csv
import hashlib
import http.client
import io
import json
import os
import socket
import ssl
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence


UTC = timezone.utc


def require(condition: bool, message: str) -> None:
    """Raise a useful validation error instead of relying on ``assert``."""
    if not condition:
        raise ValueError(message)


class IPv4HTTPConnection(http.client.HTTPConnection):
    """Connect by IPv4 only. This cluster has no IPv6 route."""

    def connect(self) -> None:
        self.sock = _connect_ipv4(self.host, self.port, self.timeout)


class IPv4HTTPSConnection(http.client.HTTPSConnection):
    """Connect by IPv4 only, then wrap TLS with the requested hostname."""

    def connect(self) -> None:
        raw = _connect_ipv4(self.host, self.port, self.timeout)
        context = self._context or ssl.create_default_context()
        self.sock = context.wrap_socket(raw, server_hostname=self.host)


class _IPv4HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, request):
        return self.do_open(IPv4HTTPConnection, request)


class _IPv4HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        # Framework Python on macOS can use a different OpenSSL CA location
        # from the operating system. Keep TLS verification enabled and use the
        # system bundle when no explicit CA path was selected by the user.
        if sys.platform == "darwin" and not os.environ.get("SSL_CERT_FILE") and not os.environ.get("SSL_CERT_DIR") and Path("/etc/ssl/cert.pem").is_file():
            context = ssl.create_default_context(cafile="/etc/ssl/cert.pem")
        else:
            context = ssl.create_default_context()
        return self.do_open(IPv4HTTPSConnection, request, context=context)


def _connect_ipv4(host: str, port: int, timeout: float | None) -> socket.socket:
    previous = socket.getdefaulttimeout()
    if timeout is not None and timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
        socket.setdefaulttimeout(timeout)
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    finally:
        socket.setdefaulttimeout(previous)
    last_error: OSError | None = None
    for family, socktype, proto, _canon, address in infos:
        sock = socket.socket(family, socktype, proto)
        try:
            if timeout is not None and timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)
            sock.connect(address)
            return sock
        except OSError as exc:
            sock.close()
            last_error = exc
    raise last_error or OSError(f"No IPv4 address for {host}")


def ipv4_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    """Open direct IPv4 HTTP(S); the custom connector cannot tunnel via a system proxy."""
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _IPv4HTTPHandler(), _IPv4HTTPSHandler(), *handlers)


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    """Parse an explicit UTC ISO-8601 timestamp."""
    require(isinstance(value, str) and value.endswith("Z"), f"Expected UTC timestamp ending in Z: {value!r}")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"Invalid UTC timestamp: {value}") from exc
    require(parsed.tzinfo is not None, f"Timestamp has no timezone: {value}")
    return parsed.astimezone(UTC)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: os.PathLike[str] | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: os.PathLike[str] | str, text: str) -> None:
    """Replace a text file in one step so a crash does not leave a partial receipt."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=target.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(text)
    temporary.replace(target)


def write_json(path: os.PathLike[str] | str, value: object) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def read_json(path: os.PathLike[str] | str) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_jsonl(path: os.PathLike[str] | str, rows: Iterable[Mapping[str, object]]) -> None:
    lines = [json.dumps(dict(row), sort_keys=True, ensure_ascii=False) + "\n" for row in rows]
    _atomic_text(path, "".join(lines))


def read_jsonl(path: os.PathLike[str] | str) -> list[dict[str, object]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        if line.strip():
            value = json.loads(line)
            require(isinstance(value, dict), f"JSONL row is not an object: {path}")
            rows.append(value)
    return rows


def write_csv(path: os.PathLike[str] | str, fields: Sequence[str], rows: Iterable[Mapping[str, object]]) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(fields), extrasaction="raise", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    _atomic_text(path, buffer.getvalue())


def read_csv(path: os.PathLike[str] | str, fields: Sequence[str] | None = None) -> list[dict[str, str]]:
    raw = Path(path).read_text(encoding="utf-8", errors="replace").replace("\x00", "")
    reader = csv.DictReader(io.StringIO(raw))
    require(reader.fieldnames is not None, f"Missing CSV header: {path}")
    if fields is not None:
        require(list(reader.fieldnames) == list(fields), f"Schema mismatch for {path}: {reader.fieldnames}")
    rows = list(reader)
    require(all(None not in row.values() for row in rows), f"Malformed CSV row in {path}")
    return rows


def atomic_write_bytes(path: os.PathLike[str] | str, data: bytes) -> None:
    """Write a response snapshot without exposing a partial file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(data)
    temporary.replace(target)


def stable_hash(value: object) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def is_json_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))
