"""Rolling live cohort: T0 is the collection start, then configured checkpoints.

The process enrolls Pump.fun creates for the configured period, requests
metadata at T0 as soon as each event is seen, then observes the same cohort
at the configured checkpoints. Chain block time is not the observation clock.

Receipts are rewritten after every enrollment batch and every checkpoint
attempt. A later call on the same directory resumes an unfinished cohort.
Discovery and observation errors are recorded and retried; they do not end
the process.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .acquire_chain import JsonRpcClient, FallbackRpcClient, decode_transaction
from .common import now_utc, parse_utc, read_csv, read_json, stable_hash, write_csv, write_json, write_jsonl
from .observe_metadata import observe_events
from .schema import ATTEMPT_FIELDS, EVENT_FIELDS, PLAN_FIELDS, SNAPSHOT_FIELDS


DEFAULT_OFFSETS = {"T0": 0, "T+24h": 24 * 3600, "T+60h": 60 * 3600}
TERMINAL_STATUSES = {"success", "not_collected_policy"}


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _offsets(protocol: dict[str, Any]) -> dict[str, int]:
    raw = protocol.get("observation", {}).get("checkpoint_offsets_seconds") or DEFAULT_OFFSETS
    return {str(name): int(seconds) for name, seconds in raw.items()}


def _plan_row(event_key: str, checkpoint: str, scheduled: datetime, protocol: dict[str, Any]) -> dict[str, str]:
    scheduled_text = _iso(scheduled)
    return {
        "event_key": event_key,
        "checkpoint": checkpoint,
        "required": "true",
        "scheduled_at": scheduled_text,
        "earliest_allowed_at": scheduled_text,
        "max_lateness_seconds": str(protocol["observation"]["max_lateness_seconds"]),
        "protocol_version": str(protocol["protocol_version"]),
        "plan_status": "scheduled",
    }


def _load_rows(path: Path, fields: list[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return read_csv(path, fields)


def _failure_count(value: Any) -> int:
    if isinstance(value, dict):
        return int(value.get("count", 0))
    return int(value or 0)


def _failure_block_time(value: Any) -> int | None:
    if isinstance(value, dict) and str(value.get("block_time", "")).isdigit():
        return int(value["block_time"])
    return None


def _remember_failure(failures: dict[str, dict[str, str]], signature: str, block_time: Any) -> None:
    previous = failures.get(signature) or {}
    remembered = block_time if block_time is not None else _failure_block_time(previous)
    failures[signature] = {"count": str(_failure_count(previous) + 1), "block_time": "" if remembered is None else str(int(remembered))}


def _publish(state: dict[str, Any], batch: list[dict[str, str]]) -> None:
    callback = state.get("on_events")
    if callback and batch:
        callback(batch)


def _checkpoint(state: dict[str, Any], failures: dict[str, dict[str, str]], page_done: set[str]) -> None:
    state["signature_failures"] = failures
    state["page_done"] = sorted(page_done)
    callback = state.get("on_checkpoint")
    if callback:
        callback()


def discover_window_events(client: JsonRpcClient, protocol: dict[str, Any], state: dict[str, Any], window_start: datetime, window_end: datetime) -> list[dict[str, str]]:
    """Poll newest program signatures and stop at the previous cursor or the window start.

    Each fully handled page is recorded in ``pending_before``. A later poll or
    a restarted process continues from that signature instead of the chain tip.
    A failed page does not move the cursor.
    """
    program_id = str(protocol.get("pumpfun_program_id"))
    events: list[dict[str, str]] = []
    failures: dict[str, dict[str, str]] = {}
    for key, value in (state.get("signature_failures") or {}).items():
        if isinstance(value, dict):
            failures[str(key)] = {"count": str(_failure_count(value)), "block_time": str(value.get("block_time", ""))}
        else:
            failures[str(key)] = {"count": str(_failure_count(value)), "block_time": ""}
    max_signature_retries = int(protocol.get("observation", {}).get("max_signature_retries", 5))
    page_done = {str(item) for item in (state.get("page_done") or [])}
    transaction_budget = max(1, int(protocol.get("acquisition", {}).get("transactions_per_poll", 10)))
    transactions_this_poll = 0
    state["_batch_yielded"] = False
    deferred = [signature for signature, record in failures.items() if _failure_count(record) < max_signature_retries]
    for signature in deferred:
        if transactions_this_poll >= transaction_budget:
            state["_scan_complete"] = False
            state["_batch_yielded"] = True
            _checkpoint(state, failures, page_done)
            return events
        try:
            transactions_this_poll += 1
            transaction = client.call("getTransaction", [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}])
        except Exception as exc:
            _remember_failure(failures, signature, None)
            print(f"transaction {signature} failed again: {exc}", flush=True)
            continue
        block_time = _failure_block_time(failures.get(signature))
        failures.pop(signature, None)
        page_done.add(signature)
        decoded = decode_transaction(transaction, signature, block_time, program_id)
        events.extend(decoded)
        _publish(state, decoded)
    _checkpoint(state, failures, page_done)

    if transactions_this_poll >= transaction_budget:
        state["_scan_complete"] = False
        state["_batch_yielded"] = True
        return events

    started_from_tip = not state.get("pending_before")
    before = state.get("pending_before") or None
    scan_tip = None if started_from_tip else state.get("scan_tip")
    pages = 0
    page_failed = False
    finished = False
    page_limit = max(1, int(protocol.get("acquisition", {}).get("page_limit", 1000)))
    max_pages = int(protocol.get("acquisition", {}).get("max_pages", 50))
    while pages < max_pages:
        options: dict[str, Any] = {"limit": page_limit}
        if before:
            options["before"] = before
        pages += 1
        try:
            response = client.call("getSignaturesForAddress", [program_id, options])
        except Exception as exc:
            print(f"signature page failed: {exc}", flush=True)
            page_failed = True
            break
        page = response.get("result") or []
        if not page:
            finished = True
            break
        if scan_tip is None:
            scan_tip = str(page[0].get("signature") or "")
        reached_previous = False
        page_events: list[dict[str, str]] = []
        for item in page:
            signature = str(item.get("signature") or "")
            if transactions_this_poll >= transaction_budget:
                state["pending_before"] = previous_signature
                state["scan_tip"] = scan_tip
                state["_scan_complete"] = False
                state["_batch_yielded"] = True
                events.extend(page_events)
                _checkpoint(state, failures, page_done)
                return events
            previous_signature = signature
            if not signature or signature == state.get("cursor"):
                reached_previous = True
                break
            if signature in page_done:
                continue
            block_time = item.get("blockTime")
            if block_time is None or item.get("err") is not None:
                page_done.add(signature)
                continue
            event_time = datetime.fromtimestamp(int(block_time), tz=timezone.utc)
            if event_time < window_start:
                reached_previous = True
                break
            if event_time >= window_end:
                page_done.add(signature)
                continue
            if _failure_count(failures.get(signature)) >= max_signature_retries:
                page_done.add(signature)
                continue
            try:
                transactions_this_poll += 1
                transaction = client.call("getTransaction", [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}])
            except Exception as exc:
                _remember_failure(failures, signature, block_time)
                print(f"transaction {signature} failed: {exc}", flush=True)
                _checkpoint(state, failures, page_done)
                continue
            failures.pop(signature, None)
            page_done.add(signature)
            decoded = decode_transaction(transaction, signature, int(block_time), program_id)
            page_events.extend(decoded)
            _publish(state, decoded)
            _checkpoint(state, failures, page_done)
        events.extend(page_events)
        if reached_previous or len(page) < page_limit:
            finished = True
            break
        before = str(page[-1].get("signature") or "") or before
        page_done.clear()
        state["pending_before"] = before
        state["scan_tip"] = scan_tip
        print(f"completed signature page; resume before {before}", flush=True)
        _checkpoint(state, failures, page_done)
    if finished and not page_failed:
        if scan_tip:
            state["cursor"] = scan_tip
        state["pending_before"] = None
        state["scan_tip"] = None
        page_done.clear()
    elif before and not page_failed:
        state["pending_before"] = before
        state["scan_tip"] = scan_tip
    state["_scan_complete"] = finished and not page_failed
    state["signature_failures"] = failures
    state["page_done"] = sorted(page_done)
    if any(_failure_count(record) >= max_signature_retries for record in failures.values()):
        write_jsonl(Path(str(state.get("_output") or ".")) / "missing_transactions.jsonl", [{"signature": signature, "attempts": str(_failure_count(record)), "block_time": record.get("block_time", "")} for signature, record in sorted(failures.items()) if _failure_count(record) >= max_signature_retries])
    _checkpoint(state, failures, page_done)
    return events


def _terminal_keys(attempts: list[dict[str, str]], rounds: dict[str, int], max_rounds: int) -> set[tuple[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in attempts:
        grouped.setdefault((row["event_key"], row["checkpoint"]), []).append(row)
    done: set[tuple[str, str]] = set()
    for key, rows in grouped.items():
        if any(row["status"] in TERMINAL_STATUSES for row in rows):
            done.add(key)
            continue
        if any(row.get("http_status") in ("401", "403") for row in rows):
            done.add(key)
            continue
        if int(rounds.get(f"{key[0]}|{key[1]}", 0)) >= max_rounds:
            done.add(key)
    for key_text, count in rounds.items():
        event_key, separator, checkpoint = key_text.partition("|")
        if separator and int(count) >= max_rounds:
            done.add((event_key, checkpoint))
    return done


def run_live_cohort(protocol: dict[str, Any], source_register: list[dict[str, str]], output_dir: str | Path, rpc_endpoint: str | None = None, transport: Callable[..., Any] | None = None, clock: Callable[[], str] | None = None, sleep: Callable[[float], None] | None = None, discover: Callable[..., list[dict[str, str]]] | None = None, rpc_client: Any = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Enroll for the configured period and retry through the final checkpoint."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    clock = clock or now_utc
    sleep = sleep or __import__("time").sleep
    offsets = _offsets(protocol)
    enrollment_seconds = int(protocol.get("observation", {}).get("enrollment_seconds", offsets.get("T+24h", 24 * 3600)))
    poll_seconds = float(protocol.get("observation", {}).get("poll_seconds", 20))
    max_rounds = int(protocol.get("observation", {}).get("max_retry_rounds", 5))

    status_path = output / "cohort_status.json"
    clock_path = output / "cohort_clock.json"
    protocol_path = output / "cohort_protocol.json"
    saved_status = read_json(status_path) if status_path.exists() else {}
    if saved_status.get("phase") == "finished" and (output / "launch_events.csv").exists():
        frozen = read_json(protocol_path) if protocol_path.exists() else dict(protocol)
        print(f"live cohort already finished with {saved_status.get('events', 0)} events", flush=True)
        return _load_rows(output / "launch_events.csv", EVENT_FIELDS), frozen

    if clock_path.exists():
        saved_clock = read_json(clock_path)
        started = parse_utc(str(saved_clock["collection_started_at"]))
        enroll_until = parse_utc(str(saved_clock["enrollment_until"]))
        print(f"resuming live cohort from {saved_clock['collection_started_at']}", flush=True)
    else:
        started = parse_utc(clock())
        enroll_until = started + timedelta(seconds=enrollment_seconds)
        write_json(clock_path, {"collection_started_at": _iso(started), "enrollment_until": _iso(enroll_until), "checkpoints": {name: _iso(started + timedelta(seconds=int(seconds))) for name, seconds in offsets.items()}, "clock": "collection_start"})

    frozen = dict(protocol)
    frozen["collection_started_at"] = _iso(started)
    frozen["window"] = {"start": _iso(started), "end": _iso(enroll_until), "inclusion": "start_inclusive_end_exclusive", "clock": "collection_start"}
    frozen["observation"] = dict(protocol.get("observation", {}))
    frozen["observation"]["checkpoint_offsets_seconds"] = offsets
    write_json(protocol_path, frozen)

    client = None
    if discover is None:
        client = rpc_client or FallbackRpcClient(frozen, rpc_endpoint, output.parent, source_register)
        discover = discover_window_events
    state: dict[str, Any] = {
        "cursor": saved_status.get("cursor"),
        "pending_before": saved_status.get("pending_before"),
        "scan_tip": saved_status.get("scan_tip"),
        "page_done": list(saved_status.get("page_done") or []),
        "signature_failures": saved_status.get("signature_failures") or {},
        "final_scan_complete": bool(saved_status.get("final_scan_complete", False)),
        "_output": str(output),
    }
    events = _load_rows(output / "launch_events.csv", EVENT_FIELDS)
    plan = _load_rows(output / "observation_plan.csv", PLAN_FIELDS)
    seen_keys = {row["event_key"] for row in events}
    rounds = {str(key): int(value) for key, value in (saved_status.get("rounds") or {}).items()}
    horizon = started + timedelta(seconds=max(offsets.values()))
    done = _terminal_keys(_load_rows(output / "request_attempts.csv", ATTEMPT_FIELDS), rounds if parse_utc(clock()) >= horizon else {}, max_rounds)

    def flush_events() -> None:
        write_csv(output / "launch_events.csv", EVENT_FIELDS, events)
        write_csv(output / "observation_plan.csv", PLAN_FIELDS, plan)

    def write_status(now: datetime, phase: str) -> None:
        write_json(status_path, {
            "phase": phase,
            "now": _iso(now),
            "events": len(events),
            "completed_observations": len(done),
            "planned_observations": len(plan),
            "cursor": state.get("cursor"),
            "pending_before": state.get("pending_before"),
            "scan_tip": state.get("scan_tip"),
            "page_done": list(state.get("page_done") or []),
            "signature_failures": state.get("signature_failures") or {},
            "final_scan_complete": bool(state.get("final_scan_complete")),
            "rounds": rounds,
            "protocol_hash": stable_hash(frozen),
        })

    def observe_due(now: datetime) -> None:
        due_names = {name for name, seconds in offsets.items() if now >= started + timedelta(seconds=int(seconds))}
        due_rows = [row for row in plan if row["checkpoint"] in due_names and (row["event_key"], row["checkpoint"]) not in done]
        if not due_rows:
            return
        print(f"observing {len(due_rows)} checkpoint rows at {_iso(now)}", flush=True)
        try:
            attempts, _snapshots = observe_events(events, due_rows, source_register, frozen, output, transport=transport, clock=clock, append=True)
        except Exception as exc:
            print(f"observation failed and will be retried: {exc.__class__.__name__}: {exc}", flush=True)
            attempts = _load_rows(output / "request_attempts.csv", ATTEMPT_FIELDS)
        closing_rounds: dict[str, int] = {}
        if now >= horizon:
            for row in due_rows:
                key = f"{row['event_key']}|{row['checkpoint']}"
                rounds[key] = int(rounds.get(key, 0)) + 1
            closing_rounds = rounds
        done.update(_terminal_keys(attempts, closing_rounds, max_rounds))

    def accept_events(batch: list[dict[str, str]]) -> None:
        added = 0
        for event in batch:
            if event.get("event_key") in seen_keys:
                continue
            seen_keys.add(event["event_key"])
            events.append(event)
            for name, seconds in offsets.items():
                plan.append(_plan_row(event["event_key"], name, started + timedelta(seconds=int(seconds)), frozen))
            added += 1
        if added:
            print(f"enrolled {added} events; cohort size {len(events)}", flush=True)
            flush_events()
            observe_due(parse_utc(clock()))

    state["on_events"] = accept_events
    state["on_checkpoint"] = lambda: write_status(parse_utc(clock()), "running")
    flush_events()
    while True:
        now = parse_utc(clock())
        retryable_transactions = any(_failure_count(row) < int(frozen["observation"].get("max_signature_retries", 5)) for row in (state.get("signature_failures") or {}).values())
        if now < enroll_until or not state["final_scan_complete"] or retryable_transactions:
            try:
                state["_scan_complete"] = True
                found = discover(client, frozen, state, started, enroll_until)
                if now >= enroll_until and state.get("_scan_complete") and not state.get("pending_before"):
                    state["final_scan_complete"] = True
            except Exception as exc:
                print(f"discovery failed and will be retried: {exc.__class__.__name__}: {exc}", flush=True)
                state["_scan_complete"] = False
                found = []
            added = 0
            for event in found:
                if event.get("event_key") in seen_keys:
                    continue
                seen_keys.add(event["event_key"])
                events.append(event)
                for name, seconds in offsets.items():
                    plan.append(_plan_row(event["event_key"], name, started + timedelta(seconds=int(seconds)), frozen))
                added += 1
            if added:
                print(f"enrolled {added} events; cohort size {len(events)}", flush=True)
                flush_events()
        now = parse_utc(clock())
        observe_due(now)
        flush_events()
        pending_times = []
        if now < enroll_until:
            pending_times.append(min(poll_seconds, max((enroll_until - now).total_seconds(), 0)))
        if not state["final_scan_complete"]:
            pending_times.append(0 if state.get("_batch_yielded") else poll_seconds)
        elif any(_failure_count(row) < int(frozen["observation"].get("max_signature_retries", 5)) for row in (state.get("signature_failures") or {}).values()):
            pending_times.append(poll_seconds)
        if now < horizon:
            pending_times.append(max((horizon - now).total_seconds(), 0))
        unfinished = [row for row in plan if (row["event_key"], row["checkpoint"]) not in done]
        for row in unfinished:
            target = parse_utc(row["scheduled_at"])
            if now < target:
                pending_times.append(max((target - now).total_seconds(), 0))
            else:
                pending_times.append(poll_seconds)
        phase = "running" if pending_times else "finished"
        write_status(now, phase)
        if not pending_times:
            break
        wait = min(pending_times)
        if wait > 0:
            print(f"waiting {wait:.0f}s", flush=True)
            sleep(wait)
    if not (output / "request_attempts.csv").exists():
        write_csv(output / "request_attempts.csv", ATTEMPT_FIELDS, [])
        write_jsonl(output / "request_attempts.jsonl", [])
    if not (output / "response_snapshots.csv").exists():
        write_csv(output / "response_snapshots.csv", SNAPSHOT_FIELDS, [])
        write_jsonl(output / "response_bodies.jsonl", [])
    write_status(parse_utc(clock()), "finished")
    print(f"live cohort finished with {len(events)} events", flush=True)
    return events, frozen
