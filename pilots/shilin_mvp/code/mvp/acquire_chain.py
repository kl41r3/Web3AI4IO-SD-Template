"""Acquire and decode Pump.fun creation events.

Inputs are a local fixture or Solana JSON-RPC. Live collection tries PublicNode
HTTPS, PublicNode HTTP/3, then the official RPC, recording each connection
choice. Outputs are launch events and acquisition receipts.
"""

from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.request
import urllib.error
from contextlib import contextmanager
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .common import ipv4_opener, now_utc, parse_utc, read_json, require, sha256_file, stable_hash, write_csv, write_json
from .schema import EVENT_FIELDS


PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DECODER_VERSION = "pumpfun-create-borsh-v2"
CREATE_DISCRIMINATORS = {bytes((24, 30, 200, 40, 5, 28, 7, 119)), bytes((214, 144, 76, 236, 95, 139, 49, 180))}


def _base58_decode(value: str) -> bytes:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = 0
    for char in value:
        require(char in alphabet, f"Invalid base58 character in instruction data: {char!r}")
        number = number * 58 + alphabet.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    return b"\x00" * (len(value) - len(value.lstrip("1"))) + raw


def _read_borsh_string(data: bytes, offset: int) -> tuple[str, int]:
    require(offset + 4 <= len(data), "Truncated Borsh string length")
    length = int.from_bytes(data[offset:offset + 4], "little")
    start = offset + 4
    end = start + length
    require(end <= len(data), "Truncated Borsh string payload")
    return data[start:end].decode("utf-8"), end


def _base58_encode(raw: bytes) -> str:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(raw, "big")
    value = ""
    while number:
        number, remainder = divmod(number, 58)
        value = alphabet[remainder] + value
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + value


def _decode_create_fields(encoded_data: str) -> tuple[str, str, str, str] | None:
    """Read create/create_v2 arguments using the official Pump.fun IDL layout."""
    try:
        raw = _base58_decode(encoded_data)
        if raw[:8] not in CREATE_DISCRIMINATORS:
            return None
        offset = 8
        name, offset = _read_borsh_string(raw, offset)
        symbol, offset = _read_borsh_string(raw, offset)
        uri, offset = _read_borsh_string(raw, offset)
        if len(raw) < offset + 32:
            return None
        return name, symbol, uri, _base58_encode(raw[offset:offset + 32])
    except (UnicodeDecodeError, ValueError, OverflowError):
        return None


def decode_create_args(encoded_data: str) -> tuple[str, str, str] | None:
    """Return the name, symbol and metadata URI of a valid creation instruction."""
    decoded = _decode_create_fields(encoded_data)
    return decoded[:3] if decoded else None


def _account_pubkey(account: Any) -> str:
    if isinstance(account, str):
        return account
    if isinstance(account, dict):
        return str(account.get("pubkey", ""))
    return ""


def _instruction_program_id(instruction: dict[str, Any], account_keys: list[str]) -> str:
    if instruction.get("programId"):
        return str(instruction["programId"])
    index = instruction.get("programIdIndex")
    if isinstance(index, int) and 0 <= index < len(account_keys):
        return account_keys[index]
    return ""


def _event_from_instruction(instruction: dict[str, Any], account_keys: list[str], program_id: str, signature: str, slot: str, block_timestamp: str, outer_index: int, inner_index: int) -> dict[str, str] | None:
    if _instruction_program_id(instruction, account_keys) != program_id:
        return None
    decoded = _decode_create_fields(str(instruction.get("data", ""))) if instruction.get("data") else None
    if decoded is None or not decoded[2]:
        return None
    accounts = instruction.get("accounts") or []
    account_values = [_account_pubkey(a) if not isinstance(a, int) else (account_keys[a] if isinstance(a, int) and a < len(account_keys) else "") for a in accounts]
    mint = account_values[0] if account_values else ""
    creator = decoded[3]
    return {
        "event_key": f"solana:{signature}:{outer_index}:{inner_index}", "network": "solana-mainnet", "platform": "pump.fun",
        "transaction_signature": signature, "slot": slot, "block_time": block_timestamp,
        "outer_instruction_index": str(outer_index), "inner_instruction_index": str(inner_index),
        "mint": mint, "creator": creator, "metadata_uri": decoded[2],
        "decoder_version": DECODER_VERSION, "event_status": "success",
    }


def decode_transaction(transaction: dict[str, Any], signature: str, block_time: int | None, program_id: str = PUMPFUN_PROGRAM_ID) -> list[dict[str, str]]:
    """Extract Pump.fun creates from top-level and inner instructions.

    An event is emitted only when the instruction data decodes to a metadata URI.
    Log text that merely contains ``Create`` is not treated as a create.
    """
    result = transaction.get("result") or transaction
    if not result:
        return []
    tx = result.get("transaction", {})
    meta = result.get("meta") or {}
    message = tx.get("message", {})
    account_keys = [_account_pubkey(k) for k in message.get("accountKeys", [])]
    slot = str(result.get("slot", ""))
    block_timestamp = "" if block_time is None else str(int(block_time))
    events: list[dict[str, str]] = []
    for outer_index, instruction in enumerate(message.get("instructions", [])):
        if isinstance(instruction, dict):
            event = _event_from_instruction(instruction, account_keys, program_id, signature, slot, block_timestamp, outer_index, 0)
            if event:
                events.append(event)
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        outer_index = int(group.get("index", 0))
        for inner_index, instruction in enumerate(group.get("instructions") or []):
            if isinstance(instruction, dict):
                event = _event_from_instruction(instruction, account_keys, program_id, signature, slot, block_timestamp, outer_index, inner_index + 1)
                if event:
                    events.append(event)
    return events


def rpc_method_intervals(protocol: dict[str, Any], endpoint: str) -> dict[str, float]:
    """Read the pacing declared for the selected endpoint only."""
    for candidate in protocol.get("api_candidates", []):
        if candidate.get("endpoint") == endpoint:
            return dict(candidate.get("rpc_method_min_interval_seconds") or {})
    return {}


class JsonRpcClient:
    """Small JSON-RPC client with explicit timeout and response accounting."""

    def __init__(self, endpoint: str, timeout: float = 20.0, opener: urllib.request.OpenerDirector | None = None, min_interval: float = 0.2, max_attempts: int = 8, method_min_intervals: dict[str, float] | None = None):
        self.endpoint = endpoint
        self.timeout = timeout
        self.opener = opener or ipv4_opener()
        self.min_interval = min_interval
        self.method_min_intervals: dict[str, float] = {}
        for method, interval in (method_min_intervals or {}).items():
            from math import isfinite
            require(not isinstance(interval, bool) and isinstance(interval, (int, float)) and isfinite(interval) and interval >= 0,
                    f"RPC interval for {method} must be a finite nonnegative number")
            self.method_min_intervals[method] = float(interval)
        self.calls = 0
        self.max_attempts = max(1, int(max_attempts))
        self.rate_delays: dict[str, float] = {}

    def wait_after_rate_limit(self, method: str, headers: Any = None) -> None:
        """Keep method-level backoff across transactions and honor Retry-After."""
        delay = self.rate_delays.get(method, 1.0)
        if headers:
            retry_after = headers.get("Retry-After", "")
            try:
                from math import isfinite
                numeric_delay = float(retry_after)
                if isfinite(numeric_delay):
                    delay = max(delay, numeric_delay)
            except (TypeError, ValueError):
                if retry_after:
                    from email.utils import parsedate_to_datetime
                    try:
                        target = parsedate_to_datetime(retry_after)
                        if target.tzinfo is None:
                            target = target.replace(tzinfo=timezone.utc)
                        delay = max(delay, (target - datetime.now(timezone.utc)).total_seconds())
                    except (TypeError, ValueError, OverflowError):
                        pass
        time.sleep(max(delay, self.method_min_intervals.get(method, self.min_interval)))
        self.rate_delays[method] = min(self.rate_delays.get(method, 1.0) * 2, 30.0)

    def call(self, method: str, params: list[Any]) -> dict[str, Any]:
        delay = 1.0
        last_error: Exception | None = None
        for _ in range(self.max_attempts):
            self.calls += 1
            payload = json.dumps({"jsonrpc": "2.0", "id": self.calls, "method": method, "params": params}).encode()
            request = urllib.request.Request(self.endpoint, data=payload, headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "mvp-metadata-observer/1.0"}, method="POST")
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    body = response.read()
            except urllib.error.HTTPError as exc:
                last_error = RuntimeError(f"RPC HTTP {exc.code}")
                print(f"rpc {method} HTTP {exc.code}; retrying", flush=True)
                if exc.code == 429:
                    self.wait_after_rate_limit(method, exc.headers)
                    continue
                if exc.code in (500, 502, 503, 504):
                    time.sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                raise last_error from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = RuntimeError(f"RPC transport error: {exc.__class__.__name__}: {exc}")
                print(f"rpc {method} transport {exc.__class__.__name__}: {exc}; retrying", flush=True)
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            try:
                value = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("RPC returned invalid JSON") from exc
            if value.get("error"):
                message = str(value["error"])
                if any(token in message.lower() for token in ("429", "too many", "rate")):
                    last_error = RuntimeError(f"RPC error: {value['error']}")
                    self.wait_after_rate_limit(method)
                    continue
                raise RuntimeError(f"RPC error: {value['error']}")
            interval = self.method_min_intervals.get(method, self.min_interval)
            if interval:
                time.sleep(interval)
            self.rate_delays.pop(method, None)
            return value
        raise last_error or RuntimeError("RPC retries exhausted")


class Http3Opener:
    """Use a verified HTTP/3 connection through JsonRpcClient's opener interface."""

    def __init__(self):
        from curl_cffi import CurlHttpVersion, CurlOpt, requests
        self.requests = requests
        self.version = CurlHttpVersion.V3ONLY
        self.session = requests.Session(trust_env=False, curl_options={CurlOpt.IPRESOLVE: 1})

    @contextmanager
    def open(self, request, timeout):
        try:
            response = self.session.request(request.get_method(), request.full_url,
                data=request.data, headers=dict(request.header_items()), timeout=timeout,
                http_version=self.version, verify=True, allow_redirects=False)
        except self.requests.RequestsError as exc:
            raise urllib.error.URLError(exc) from exc
        if response.http_version != 30:
            raise urllib.error.URLError("The requested HTTP/3 connection was unavailable")
        if response.status_code >= 300:
            raise urllib.error.HTTPError(request.full_url, response.status_code,
                "RPC HTTP error", response.headers, BytesIO(response.content))
        with BytesIO(response.content) as body:
            yield body

    def close(self):
        self.session.close()


class FallbackRpcClient:
    """Try the configured connection order and keep a working route for this run."""

    def __init__(self, protocol: dict[str, Any], endpoint: str | None = None,
                 output_dir: str | Path | None = None, source_register: list[dict] | None = None,
                 client_factory: Callable[..., Any] | None = None):
        self.protocol = protocol
        candidates = [dict(row) for row in protocol["api_candidates"] if row.get("access_decision") == "allowed"]
        primary = "https://solana-rpc.publicnode.com"
        if not endpoint or endpoint == primary:
            # Older saved cohorts keep their sampling clock and gain the current connection order.
            registered = protocol["api_candidates"]
            first = next((dict(row) for row in registered if row.get("endpoint") == primary),
                         {"candidate_id": "publicnode-solana-mainnet", "endpoint": primary})
            first["transport"] = "https"
            second = dict(first)
            second.update(candidate_id="publicnode-solana-mainnet-http3", transport="http3")
            official_url = "https://api.mainnet-beta.solana.com"
            third = next((dict(row) for row in registered if row.get("endpoint") == official_url),
                         {"candidate_id": "solana-official-rpc-secondary-region", "endpoint": official_url})
            third.update(transport="https", rpc_method_min_interval_seconds={"getTransaction": 3.0})
            candidates = [first, second, third]
        else:
            candidates = [row for row in candidates if row.get("endpoint") == endpoint] or [
                {"candidate_id": "user-selected-rpc", "endpoint": endpoint, "transport": "https"}]
        require(bool(candidates), "The protocol needs an allowed RPC connection")
        self.routes = candidates
        self.index = 0
        self.client = None
        self.selected = False
        self.completed_calls = 0
        self.output = Path(output_dir) if output_dir is not None else None
        self.sources = source_register
        self.factory = client_factory
        self.history = []
        if self.output:
            self.output.mkdir(parents=True, exist_ok=True)
            prior = self.output / "API_SELECTION.json"
            if prior.exists():
                self.history = list(read_json(prior).get("connection_history") or [])

    @property
    def endpoint(self):
        return self.routes[self.index]["endpoint"]

    @property
    def calls(self):
        return self.completed_calls + (self.client.calls if self.client else 0)

    def _record(self, status: str, method: str, error: str = "") -> None:
        route = self.routes[self.index]
        entry = {"utc": now_utc(), "candidate_id": route["candidate_id"],
                 "endpoint": route["endpoint"], "transport": route.get("transport", "https"),
                 "status": status, "method": method, "error": error}
        self.history.append(entry)
        if not self.output:
            return
        with (self.output / "rpc_connection_attempts.jsonl").open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")
        if status != "selected":
            path = self.output / "API_SELECTION.json"
            if path.exists():
                selection = read_json(path)
                selection["connection_history"] = self.history
                write_json(path, selection)
            return
        interval = float(route.get("rpc_method_min_interval_seconds", {}).get("getTransaction", 0.2))
        selection = {"status": "direct_collection", "connection_policy": "ordered_fallback",
            "selected_candidate": route["candidate_id"], "selected_endpoint": route["endpoint"],
            "transport": route.get("transport", "https"), "address_family": "ipv4",
            "getTransaction_pause_seconds": interval, "evidence_file": "api_benchmark.csv",
            "evidence_row": 2, "connection_order": [row["candidate_id"] for row in self.routes],
            "connection_history": self.history}
        write_json(self.output / "API_SELECTION.json", selection)
        (self.output / "API_SELECTION.md").write_text(
            "# RPC connection\n\n" + f"- Endpoint: {route['endpoint']}\n"
            + f"- Connection: {route.get('transport', 'https')}\n"
            + f"- Wait after a successful transaction query: {interval:g} seconds\n"
            + "- Connection attempts: rpc_connection_attempts.jsonl\n", encoding="utf-8")
        write_csv(self.output / "api_benchmark.csv", ["candidate_id", "endpoint", "transport", "status", "access_decision", "address_family"],
            [{"candidate_id": route["candidate_id"], "endpoint": route["endpoint"],
              "transport": route.get("transport", "https"), "status": "direct_collection", "access_decision": "allowed", "address_family": "ipv4"}])
        if self.sources:
            for row in self.sources:
                if row.get("endpoint") == route["endpoint"]:
                    row.update(automatic_access_decision="allowed", temporary_retention_decision="retain run receipt only",
                               reproduction_mode="live_rerun_only", checked_at=datetime.now(timezone.utc).date().isoformat(),
                               query_scope=f"Solana transaction queries over IPv4 using {route.get('transport', 'https')}")
            write_csv(self.output / "source_register.csv", list(self.sources[0]), self.sources)

    def call(self, method: str, params: list[Any]) -> dict[str, Any]:
        while True:
            route = self.routes[self.index]
            try:
                if self.client is None:
                    print(f"Trying RPC: {route['endpoint']} via {route.get('transport', 'https')}", flush=True)
                    if self.factory:
                        self.client = self.factory(route)
                    else:
                        opener = Http3Opener() if route.get("transport") == "http3" else None
                        self.client = JsonRpcClient(route["endpoint"], timeout=5, max_attempts=2, opener=opener,
                            method_min_intervals=route.get("rpc_method_min_interval_seconds"))
                value = self.client.call(method, params)
            except (RuntimeError, OSError, ImportError) as exc:
                self._record("failed", method, str(exc))
                if self.index + 1 >= len(self.routes):
                    raise
                self.close()
                if self.client:
                    self.completed_calls += self.client.calls
                self.client = None
                self.selected = False
                self.index += 1
                print("Trying the next RPC connection.", flush=True)
                continue
            if not self.selected:
                self._record("selected", method)
                self.selected = True
                pause = route.get("rpc_method_min_interval_seconds", {}).get("getTransaction", 0.2)
                print(f"RPC connected via {route.get('transport', 'https')}; transaction-query pause: {pause:g}s", flush=True)
            return value

    def close(self):
        if self.client and isinstance(self.client.opener, Http3Opener):
            self.client.opener.close()


def load_fixture(path: str | Path) -> list[dict[str, str]]:
    """Load a frozen event fixture and enforce the release schema."""
    value = read_json(path)
    require(isinstance(value, list), "Fixture must be a JSON array")
    events = [{field: str(row.get(field, "")) for field in EVENT_FIELDS} for row in value if isinstance(row, dict)]
    require(len(events) == len(value), "Fixture contains a non-object row")
    return events


def enumerate_rpc_events(client: JsonRpcClient, protocol: dict[str, Any], max_pages: int = 5000) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Enumerate signatures then fetch transactions until the frozen window is covered."""
    start = parse_utc(protocol["window"]["start"])
    end = parse_utc(protocol["window"]["end"])
    program_id = str(protocol.get("pumpfun_program_id", PUMPFUN_PROGRAM_ID))
    require(start < end, "Protocol window must be increasing")
    before: str | None = None
    signatures_seen = 0
    tx_seen = 0
    events: list[dict[str, str]] = []
    pages = 0
    oldest_seen: int | None = None
    reached_window_start = False
    tx_error_count = 0
    tx_errors: list[dict[str, str]] = []
    while pages < max_pages:
        options: dict[str, Any] = {"limit": 1000}
        if before:
            options["before"] = before
        print(f"rpc page {pages + 1} starting", flush=True)
        response = client.call("getSignaturesForAddress", [program_id, options])
        page = response.get("result") or []
        pages += 1
        if not page:
            break
        for item in page:
            signatures_seen += 1
            block_time = item.get("blockTime")
            if block_time is None:
                continue
            oldest_seen = int(block_time) if oldest_seen is None else min(oldest_seen, int(block_time))
            event_time = __import__("datetime").datetime.fromtimestamp(int(block_time), tz=__import__("datetime").timezone.utc)
            if event_time < start:
                before = item.get("signature")
                reached_window_start = True
                break
            if event_time >= end or item.get("err") is not None:
                continue
            signature = str(item.get("signature", ""))
            try:
                transaction = client.call("getTransaction", [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}])
            except RuntimeError as exc:
                tx_error_count += 1
                if len(tx_errors) < 50:
                    tx_errors.append({"signature": signature, "error": str(exc)[:300]})
                continue
            tx_seen += 1
            events.extend(decode_transaction(transaction, signature, int(block_time), program_id))
        else:
            before = page[-1].get("signature")
            print(f"rpc page {pages} signatures={signatures_seen} events={len(events)}", flush=True)
            continue
        if oldest_seen is not None and event_time < start:
            break
    # DGP-ACQ-001: event denominator is defined by event key and frozen chain-time window.
    unique: dict[str, dict[str, str]] = {}
    for event in events:
        if not event["block_time"]:
            continue
        event_time = datetime.fromtimestamp(int(event["block_time"]), tz=timezone.utc)
        if start <= event_time < end and event["event_key"] not in unique:
            unique[event["event_key"]] = event
    return sorted(unique.values(), key=lambda row: (int(row["block_time"] or 0), row["event_key"])), {"pages": pages, "signatures_seen": signatures_seen, "transactions_fetched": tx_seen, "rpc_calls": client.calls, "transaction_errors": tx_error_count, "transaction_error_sample": tx_errors, "reached_window_start": reached_window_start, "scan_truncated": not reached_window_start}


def acquire_events(protocol: dict[str, Any], output_dir: str | Path, fixture: str | Path | None = None, rpc_endpoint: str | None = None) -> list[dict[str, str]]:
    """Acquire events from a frozen fixture or an approved RPC endpoint."""
    output = Path(output_dir)
    if fixture:
        events = load_fixture(fixture)
        stats = {"mode": "fixture", "fixture_sha256": sha256_file(fixture), "rpc_calls": 0}
    else:
        require(rpc_endpoint, "An approved RPC endpoint is required when fixture is absent")
        client = JsonRpcClient(rpc_endpoint, method_min_intervals=rpc_method_intervals(protocol, rpc_endpoint))
        events, stats = enumerate_rpc_events(client, protocol)
        stats.update({"mode": "rpc", "endpoint": rpc_endpoint})
    window_start = parse_utc(protocol["window"]["start"])
    window_end = parse_utc(protocol["window"]["end"])
    accepted: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    seen: set[str] = set()
    for event in events:
        reason = ""
        if not event.get("event_key"):
            reason = "missing_event_key"
        elif event["event_key"] in seen:
            reason = "duplicate_event_key"
        elif event.get("network") != "solana-mainnet" or event.get("platform") != "pump.fun":
            reason = "unexpected_scope"
        elif event.get("event_status") != "success":
            reason = "non_success"
        elif not str(event.get("block_time", "")).isdigit():
            reason = "missing_block_time"
        else:
            event_time = datetime.fromtimestamp(int(event["block_time"]), tz=timezone.utc)
            if not (window_start <= event_time < window_end):
                reason = "outside_window"
        if reason:
            skipped.append({"event_key": event.get("event_key", ""), "reason": reason})
            continue
        seen.add(event["event_key"])
        accepted.append(event)
    write_csv(output / "launch_events.csv", EVENT_FIELDS, accepted)
    stats["skipped_events"] = len(skipped)
    stats["skipped_event_sample"] = skipped[:50]
    write_json(output / "acquisition_receipt.json", {"created_at": now_utc(), "protocol_hash": stable_hash(protocol), "event_count": len(accepted), "stats": stats})
    return accepted


def benchmark_candidates(protocol: dict[str, Any], candidates: list[dict[str, Any]], output: str | Path, fixture: str | Path | None = None) -> list[dict[str, Any]]:
    """Run a small, comparable benchmark and emit ``api_benchmark.csv``.

    A fixture benchmark records deterministic evidence without making network
    calls. Live candidates are opt-in and use the same frozen protocol window.
    """
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        started = time.perf_counter()
        row = {"candidate_id": candidate.get("candidate_id", ""), "endpoint": candidate.get("endpoint", ""), "role": candidate.get("role", ""), "access_decision": candidate.get("access_decision", "pending_benchmark"), "mode": "fixture" if fixture else "live", "events": "", "duplicates": "", "status": "", "latency_ms": "", "p50_latency_ms": "", "p95_latency_ms": "", "request_bytes": "", "rate_limit": "unknown", "estimated_cost": "not_recorded", "boundary_difference": "not_compared", "error": "", "evidence_protocol_hash": stable_hash(protocol)}
        try:
            if fixture:
                events = load_fixture(fixture)
                row.update(events=len(events), duplicates=len(events) - len({e["event_key"] for e in events}), status="ok")
            else:
                client = JsonRpcClient(str(candidate["endpoint"]))
                events, stats = enumerate_rpc_events(client, protocol, max_pages=2)
                row.update(events=len(events), duplicates=0, status="ok", rpc_calls=stats["rpc_calls"])
        except Exception as exc:  # evidence is recorded, candidate is not silently promoted
            row.update(status="failed", error=exc.__class__.__name__ + ": " + str(exc)[:200])
        row["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
        row["p50_latency_ms"] = row["latency_ms"]
        row["p95_latency_ms"] = row["latency_ms"]
        rows.append(row)
    fields = sorted({key for row in rows for key in row})
    write_csv(output, fields, rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fixture")
    parser.add_argument("--rpc-endpoint")
    args = parser.parse_args()
    acquire_events(read_json(args.protocol), args.output, args.fixture, args.rpc_endpoint)


if __name__ == "__main__":
    main()
