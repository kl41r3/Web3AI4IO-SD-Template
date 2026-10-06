"""Observe declared metadata URIs and record every route that was actually used.

``http(s)`` declarations are checked against policy first and requested when
allowed. A refused route gets a policy receipt without a network call.
``ipfs://`` and ``ar://``
references stay in the receipt as the original URI; the bytes are fetched
through registered gateways and stored as ``request_url`` plus ``route_id``.
Connections are IPv4-only. Public hosts may be requested without a prior
source-register row. Local and private hosts are refused. A failed route is
retried, then an approved alternate gateway is tried. Nothing in this module
waits for a person.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .common import ipv4_opener, now_utc, parse_utc, read_csv, read_json, read_jsonl, require, sha256_bytes, stable_hash, write_csv, write_json, write_jsonl
from .schema import ATTEMPT_FIELDS, EVENT_FIELDS, PLAN_FIELDS, SNAPSHOT_FIELDS, REQUIRED_CHECKPOINTS


@dataclass
class FetchResult:
    status: str
    http_status: str = ""
    body: bytes = b""
    content_type: str = ""
    redirect_location: str = ""
    retry_after_seconds: str = ""
    error_class: str = ""
    error_message: str = ""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HttpTransport:
    def __init__(self, timeout: float = 20.0, max_bytes: int = 1_000_000):
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.opener = ipv4_opener(NoRedirect())

    def __call__(self, uri: str) -> FetchResult:
        request = urllib.request.Request(uri, headers={"Accept": "application/json", "User-Agent": "mvp-metadata-observer/1.0"}, method="GET")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                body = response.read(self.max_bytes + 1)
                if len(body) > self.max_bytes:
                    return FetchResult(status="transport_error", error_class="response_too_large", error_message=str(self.max_bytes))
                return FetchResult(status="success", http_status=str(response.status), body=body, content_type=response.headers.get("Content-Type", ""))
        except urllib.error.HTTPError as exc:
            retry_after = exc.headers.get("Retry-After", "") if exc.headers else ""
            if exc.code in (301, 302, 303, 307, 308):
                return FetchResult(status="http_error", http_status=str(exc.code), redirect_location=exc.headers.get("Location", "") if exc.headers else "", error_class="redirect", error_message="redirect not followed", retry_after_seconds=retry_after)
            return FetchResult(status="success" if exc.code == 200 else "http_error", http_status=str(exc.code), retry_after_seconds=retry_after, error_class="http_error", error_message=str(exc))
        except urllib.error.URLError as exc:
            reason = str(getattr(exc, "reason", exc))
            if "timed out" in reason.lower():
                return FetchResult(status="timeout", error_class="timeout", error_message=reason)
            return FetchResult(status="transport_error", error_class="url_error", error_message=reason)
        except (TimeoutError, OSError) as exc:
            return FetchResult(status="timeout" if isinstance(exc, TimeoutError) else "transport_error", error_class=exc.__class__.__name__, error_message=str(exc))


class FixtureTransport:
    """Deterministic transport used only for local replay/demo runs."""

    def __init__(self, uri_to_path: dict[str, str | Path]):
        self.uri_to_path = {key: Path(value) for key, value in uri_to_path.items()}

    def __call__(self, uri: str) -> FetchResult:
        path = self.uri_to_path.get(uri)
        if path is None:
            return FetchResult(status="http_error", http_status="404", error_class="fixture_missing", error_message="No fixture for exact URI")
        try:
            body = path.read_bytes()
        except OSError as exc:
            return FetchResult(status="transport_error", error_class="fixture_read_error", error_message=str(exc))
        return FetchResult(status="success", http_status="200", body=body, content_type="application/json")


def build_observation_plan(events: list[dict[str, str]], protocol: dict[str, Any], anchor_time: str | None = None) -> list[dict[str, str]]:
    offsets = {"T0": 0, "T+24h": 24 * 3600, "T+60h": 60 * 3600}
    configured = protocol.get("observation", {}).get("checkpoint_offsets_seconds")
    if isinstance(configured, dict) and configured:
        offsets = {str(name): int(seconds) for name, seconds in configured.items()}
    max_lateness = int(protocol["observation"]["max_lateness_seconds"])
    rows: list[dict[str, str]] = []
    checkpoints = tuple(protocol.get("observation", {}).get("checkpoints") or REQUIRED_CHECKPOINTS)
    for event in events:
        require(event.get("block_time", "").isdigit(), f"Event lacks numeric block_time: {event['event_key']}")
        # Live cohorts pass collection start as the anchor. Fixture plans use chain time.
        chain_time = parse_utc(anchor_time) if anchor_time else __import__("datetime").datetime.fromtimestamp(int(event["block_time"]), tz=__import__("datetime").timezone.utc)
        for checkpoint in checkpoints:
            scheduled = chain_time + __import__("datetime").timedelta(seconds=offsets[checkpoint])
            scheduled_text = scheduled.isoformat(timespec="seconds").replace("+00:00", "Z")
            rows.append({"event_key": event["event_key"], "checkpoint": checkpoint, "required": "true", "scheduled_at": scheduled_text, "earliest_allowed_at": scheduled_text, "max_lateness_seconds": str(max_lateness), "protocol_version": str(protocol["protocol_version"]), "plan_status": "scheduled"})
    return rows


def _is_local_or_private_host(hostname: str) -> bool:
    name = hostname.strip("[]").lower().rstrip(".")
    if name in {"localhost", "localhost.localdomain"} or name.endswith(".localhost") or name.endswith(".local"):
        return True
    try:
        address = ipaddress.ip_address(name)
    except ValueError:
        return False
    return address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_multicast or address.is_unspecified


def _host_allowed(uri: str, source_register: list[dict[str, str]]) -> tuple[bool, dict[str, str] | None, str]:
    """Allow a public http(s) host. Refuse local, private, and explicitly denied hosts."""
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        return False, None, "invalid_uri_scheme_or_host"
    if _is_local_or_private_host(parsed.hostname):
        return False, None, "local_or_private_host"
    matched = next((source for source in source_register if source.get("host") == parsed.hostname), None)
    if matched and matched.get("automatic_access_decision") not in ("", "allowed"):
        return False, matched, "source_register_denied"
    return True, matched, ""


def metadata_request_candidates(uri: str, protocol: dict[str, Any]) -> list[tuple[str, str]]:
    """Resolve a declared metadata reference to ordered request routes.

    The original declaration is never replaced in the receipt. Alternate
    gateways follow an earlier route's policy refusal or retryable request
    failure. Nonretryable HTTP failures stop that observation.
    When a live preflight confirmed a subset of gateways, only that subset is
    used as a fallback.
    """
    parsed = urllib.parse.urlparse(uri)
    candidates: list[tuple[str, str]] = []
    gateways = protocol.get("metadata", {}).get("gateway_candidates", [])
    if parsed.scheme in ("http", "https"):
        candidates.append((uri, "declared_exact"))
        if parsed.path.startswith("/ipfs/"):
            suffix = parsed.path.split("/ipfs/", 1)[1]
            if parsed.query:
                suffix += "?" + parsed.query
            for route in gateways:
                base = str(route.get("base_url", "")).rstrip("/")
                if base and urllib.parse.urlparse(base).hostname != parsed.hostname:
                    candidates.append((base + "/ipfs/" + suffix, str(route.get("route_id", "gateway"))))
    elif parsed.scheme == "ipfs":
        suffix = (parsed.netloc + parsed.path).lstrip("/")
        if suffix:
            for route in gateways:
                base = str(route.get("base_url", "")).rstrip("/")
                if base:
                    candidates.append((base + "/ipfs/" + suffix, str(route.get("route_id", "gateway"))))
    elif parsed.scheme == "ar":
        suffix = (parsed.netloc + parsed.path).lstrip("/")
        base = str(protocol.get("metadata", {}).get("arweave_base_url", "https://arweave.net")).rstrip("/")
        if suffix:
            candidates.append((base + "/" + suffix, "arweave_gateway"))
    return _apply_confirmed_gateways(uri, candidates, protocol)


def _apply_confirmed_gateways(uri: str, candidates: list[tuple[str, str]], protocol: dict[str, Any]) -> list[tuple[str, str]]:
    confirmed = [item for item in (protocol.get("metadata_gateways") or {}).get("confirmed") or [] if item.get("status") == "json_ok"]
    if not confirmed:
        return candidates
    from .gateway_preflight import extract_ipfs_cid, gateway_url

    allowed_hosts = {str(item.get("host") or "") for item in confirmed}
    kept = [(url, route_id) for url, route_id in candidates if route_id == "declared_exact" or (urllib.parse.urlparse(url).hostname or "") in allowed_hosts]
    existing = {url for url, _route in kept}
    cid = extract_ipfs_cid(uri)
    if not cid:
        return kept
    for item in confirmed:
        url = gateway_url(str(item.get("template") or ""), cid)
        host = str(item.get("host") or "confirmed_gateway")
        if url and url not in existing:
            kept.append((url, host))
            existing.add(url)
    return kept


def _retry_after(value: str) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0


def _timing_state(plan: dict[str, str], completed_at: str, status: str) -> str:
    if status == "not_collected_policy":
        return "not_applicable"
    completed = parse_utc(completed_at)
    earliest = parse_utc(plan["earliest_allowed_at"])
    scheduled = parse_utc(plan["scheduled_at"])
    lateness = int(plan["max_lateness_seconds"])
    if completed < earliest:
        return "early_prohibited"
    if completed > scheduled + __import__("datetime").timedelta(seconds=lateness):
        return "late"
    return "on_time"


def _attempt_row(request_id: str, event_key: str, checkpoint: str, uri: str, request_url: str, route_id: str, attempt: str, started_at: str, completed_at: str, result: FetchResult | None, status: str, error_class: str = "", error_message: str = "", policy_state: str = "") -> dict[str, Any]:
    body = result.body if result else b""
    return {
        "request_id": request_id,
        "event_key": event_key,
        "checkpoint": checkpoint,
        "uri": uri,
        "request_url": request_url,
        "route_id": route_id,
        "host": urllib.parse.urlparse(request_url).hostname or "",
        "attempt": attempt,
        "started_at": started_at,
        "completed_at": completed_at,
        "available_at": completed_at if body else "",
        "status": status,
        "http_status": result.http_status if result else "",
        "redirect_location": result.redirect_location if result else "",
        "response_bytes": str(len(body)),
        "response_sha256": sha256_bytes(body) if body else "",
        "retry_after_seconds": result.retry_after_seconds if result else "",
        "error_class": error_class or (result.error_class if result else ""),
        "error_message": error_message or (result.error_message if result else ""),
        "policy_state": policy_state,
    }


def _store_success(snapshots: list[dict[str, str]], bodies: list[dict[str, Any]], request_id: str, event_key: str, checkpoint: str, uri: str, request_url: str, route_id: str, completed_at: str, result: FetchResult) -> bool:
    try:
        json.loads(result.body.decode("utf-8"))
        parse_state = "json_ok"
    except (UnicodeDecodeError, json.JSONDecodeError):
        parse_state = "parse_error"
    snapshots.append({"request_id": request_id, "event_key": event_key, "checkpoint": checkpoint, "uri": uri, "request_url": request_url, "route_id": route_id, "retrieved_at": completed_at, "content_type": result.content_type, "response_sha256": sha256_bytes(result.body), "response_bytes": str(len(result.body)), "parse_state": parse_state, "integrity_state": "sha256_recorded"})
    bodies.append({"request_id": request_id, "body_b64": __import__("base64").b64encode(result.body).decode("ascii")})
    return parse_state == "json_ok"


def _retryable(result: FetchResult) -> bool:
    if result.http_status in ("401", "403"):
        return False
    if result.status in ("timeout", "transport_error"):
        return True
    if result.http_status == "429":
        return True
    return bool(result.http_status.isdigit() and 500 <= int(result.http_status) <= 599)


def observe_events(events: list[dict[str, str]], plan: list[dict[str, str]], source_register: list[dict[str, str]], protocol: dict[str, Any], output_dir: str | Path, transport: Callable[[str], FetchResult] | None = None, clock: Callable[[], str] = now_utc, append: bool = False) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Run the URI observation schedule and write receipts after every row."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    transport = transport or HttpTransport(max_bytes=int(protocol["observation"]["max_response_bytes"]))
    events_by_key = {event["event_key"]: event for event in events}
    plan_rows = [{**row, "_event": events_by_key[row["event_key"]]} for row in plan]
    attempts: list[dict[str, Any]] = read_csv(output / "request_attempts.csv", ATTEMPT_FIELDS) if append and (output / "request_attempts.csv").exists() else []
    snapshots: list[dict[str, str]] = read_csv(output / "response_snapshots.csv", SNAPSHOT_FIELDS) if append and (output / "response_snapshots.csv").exists() else []
    bodies: list[dict[str, Any]] = read_jsonl(output / "response_bodies.jsonl") if append and (output / "response_bodies.jsonl").exists() else []
    max_attempts = int(protocol["observation"]["max_attempts"])
    max_sleep = float(protocol["observation"]["max_retry_sleep_seconds"])

    def flush() -> None:
        write_csv(output / "request_attempts.csv", ATTEMPT_FIELDS, attempts)
        write_jsonl(output / "request_attempts.jsonl", attempts)
        write_csv(output / "response_snapshots.csv", SNAPSHOT_FIELDS, snapshots)
        write_jsonl(output / "response_bodies.jsonl", bodies)
        write_json(output / "observation_receipt.json", {"created_at": now_utc(), "protocol_hash": stable_hash(protocol), "attempt_count": len(attempts), "success_count": sum(row["status"] == "success" for row in attempts), "policy_count": sum(row["status"] == "not_collected_policy" for row in attempts)})

    for planned in plan_rows:
        event = planned["_event"]
        uri = event.get("metadata_uri", "")
        request_id = stable_hash({"event_key": event["event_key"], "checkpoint": planned["checkpoint"], "uri": uri})[:24]
        candidates = metadata_request_candidates(uri, protocol) if uri else []
        if not candidates:
            attempts.append(_attempt_row(request_id, event["event_key"], planned["checkpoint"], uri, "", "", "0", "", "", None, "not_collected_policy", "missing_exact_uri" if not uri else "unsupported_metadata_scheme", "No request route exists for the declared URI", "not_collected_policy"))
            flush()
            continue
        route_succeeded = False
        attempt_counter = max((int(row["attempt"]) for row in attempts if row["request_id"] == request_id), default=0)
        for route_index, (request_url, route_id) in enumerate(candidates):
            allowed, _source, reason = _host_allowed(request_url, source_register)
            if not allowed:
                attempt_counter += 1
                status = "not_collected_policy" if route_index == len(candidates) - 1 else "route_refused_policy"
                attempts.append(_attempt_row(request_id, event["event_key"], planned["checkpoint"], uri, request_url, route_id, str(attempt_counter), "", "", None, status, reason, "Request route was refused before any network call", reason))
                flush()
                continue
            last_result: FetchResult | None = None
            for local_attempt in range(1, max_attempts + 1):
                attempt_counter += 1
                started_at = clock()
                try:
                    result = transport(request_url)
                except Exception as exc:
                    result = FetchResult(status="transport_error", error_class=exc.__class__.__name__, error_message=str(exc)[:300])
                completed_at = clock()
                last_result = result
                status = "http_error" if result.status == "success" and result.http_status and result.http_status != "200" else result.status
                attempts.append(_attempt_row(request_id, event["event_key"], planned["checkpoint"], uri, request_url, route_id, str(attempt_counter), started_at, completed_at, result, status))
                flush()
                if status == "success" and result.body and _store_success(snapshots, bodies, request_id, event["event_key"], planned["checkpoint"], uri, request_url, route_id, completed_at, result):
                    route_succeeded = True
                    flush()
                    break
                if not _retryable(result) or local_attempt >= max_attempts:
                    break
                delay = min(_retry_after(result.retry_after_seconds) or min(2 ** (local_attempt - 1), max_sleep), max_sleep)
                if delay:
                    time.sleep(delay)
            if route_succeeded:
                break
            if last_result is None or not _retryable(last_result):
                break
    flush()
    return attempts, snapshots


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--source-register", required=True)
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    events = read_csv(args.events, EVENT_FIELDS)
    plan = read_csv(args.plan, PLAN_FIELDS)
    sources = read_csv(args.source_register)
    observe_events(events, plan, sources, read_json(args.protocol), args.output)


if __name__ == "__main__":
    main()
