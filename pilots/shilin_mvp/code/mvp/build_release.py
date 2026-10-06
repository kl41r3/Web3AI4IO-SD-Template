"""Parse observations, build coverage ledgers, validate and publish an MVP release."""

from __future__ import annotations

import argparse
import base64
import json
from collections import Counter
from pathlib import Path
from typing import Any

from .common import now_utc, parse_utc, read_csv, read_json, read_jsonl, sha256_bytes, sha256_file, stable_hash, write_csv, write_json
from .schema import ATTEMPT_FIELDS, EVENT_FIELDS, FIELD_FIELDS, LEDGER_FIELDS, PLAN_FIELDS, SNAPSHOT_FIELDS, ALLOWED_FIELD_NAMES, ALLOWED_FIELD_TYPES, REQUIRED_CHECKPOINTS, TERMINAL_LEDGER_STATES


def _value_type(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _field_value(value: object) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    if value is None:
        return "null"
    return str(value)


def parse_field_observations(events: list[dict[str, str]], snapshots: list[dict[str, str]], bodies: list[dict[str, Any]], protocol: dict[str, Any]) -> list[dict[str, str]]:
    body_by_id = {str(row["request_id"]): base64.b64decode(str(row["body_b64"])) for row in bodies}
    output: list[dict[str, str]] = []
    required_fields = tuple(protocol["metadata"]["allowed_fields"])
    for snapshot in snapshots:
        if snapshot["parse_state"] != "json_ok" or snapshot["request_id"] not in body_by_id:
            continue
        try:
            document = json.loads(body_by_id[snapshot["request_id"]].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(document, dict):
            continue
        for field_name in required_fields:
            # DGP-REL-001: parser emits one row per response x pre-registered field; absent is a measured state.
            present = field_name in document
            value = document.get(field_name)
            value_type = _value_type(value) if present else "absent"
            value_text = _field_value(value) if present else ""
            output.append({"request_id": snapshot["request_id"], "event_key": snapshot["event_key"], "checkpoint": snapshot["checkpoint"], "uri": snapshot["uri"], "field_name": field_name, "json_pointer": "/" + field_name, "presence": "present" if present else "absent", "value_type": value_type, "allowed_value": value_text if present and value_type == ALLOWED_FIELD_TYPES[field_name] else "", "value_hash": stable_hash(value) if present else "", "parser_version": str(protocol["metadata"]["parser_version"])})
    return output


def _last_attempts(attempts: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    chosen: dict[str, dict[str, str]] = {}
    for row in attempts:
        request_id = row["request_id"]
        if request_id not in chosen or int(row["attempt"]) >= int(chosen[request_id]["attempt"]):
            chosen[request_id] = row
    return chosen


def build_coverage_ledger(plan: list[dict[str, str]], attempts: list[dict[str, str]], snapshots: list[dict[str, str]], fields: list[dict[str, str]]) -> list[dict[str, str]]:
    last = _last_attempts(attempts)
    snapshot_by_request = {row["request_id"]: row for row in snapshots}
    fields_by_request: dict[str, list[dict[str, str]]] = {}
    for row in fields:
        fields_by_request.setdefault(row["request_id"], []).append(row)
    ledger: list[dict[str, str]] = []
    for row in plan:
        matching = [attempt for attempt in attempts if attempt["event_key"] == row["event_key"] and attempt["checkpoint"] == row["checkpoint"]]
        successful = [attempt for attempt in matching if snapshot_by_request.get(attempt["request_id"], {}).get("parse_state") == "json_ok"]
        request_id = (successful[-1] if successful else matching[-1] if matching else {}).get("request_id", "")
        attempt = last.get(request_id)
        if not attempt:
            ledger.append({"event_key": row["event_key"], "checkpoint": row["checkpoint"], "required": row["required"], "schedule_state": "scheduled", "request_state": "not_attempted", "parse_state": "not_attempted", "field_state": "not_attempted", "timing_state": "missed", "final_state": "missed", "request_id": ""})
            continue
        status = attempt["status"]
        request_state = {"success": "success", "not_collected_policy": "not_collected_policy", "http_error": "failed", "timeout": "failed", "transport_error": "failed"}.get(status, "failed")
        snapshot = snapshot_by_request.get(request_id)
        parse_state = snapshot["parse_state"] if snapshot else ("not_collected_policy" if status == "not_collected_policy" else "not_observed")
        field_rows = fields_by_request.get(request_id, [])
        field_state = "observed" if field_rows else ("parse_failed" if parse_state == "parse_error" else "not_observed")
        completed = attempt.get("completed_at", "")
        timing_state = "not_applicable" if status == "not_collected_policy" else ("missing_time" if not completed else "unknown")
        if completed:
            try:
                timing_state = "on_time" if parse_utc(completed) >= parse_utc(row["earliest_allowed_at"]) and parse_utc(completed) <= parse_utc(row["scheduled_at"]) + __import__("datetime").timedelta(seconds=int(row["max_lateness_seconds"])) else ("early_prohibited" if parse_utc(completed) < parse_utc(row["earliest_allowed_at"]) else "late")
            except ValueError:
                timing_state = "invalid_time"
        if status == "not_collected_policy":
            final_state = "not_collected_policy"
        elif status != "success":
            final_state = "request_failed"
        elif parse_state != "json_ok":
            final_state = "parse_failed"
        elif timing_state == "early_prohibited":
            final_state = "early_prohibited"
        else:
            final_state = "observed"
        ledger.append({"event_key": row["event_key"], "checkpoint": row["checkpoint"], "required": row["required"], "schedule_state": "scheduled", "request_state": request_state, "parse_state": parse_state, "field_state": field_state, "timing_state": timing_state, "final_state": final_state, "request_id": request_id})
    return ledger


def validate_release(events: list[dict[str, str]], plan: list[dict[str, str]], attempts: list[dict[str, str]], snapshots: list[dict[str, str]], fields: list[dict[str, str]], ledger: list[dict[str, str]], bodies: list[dict[str, Any]], checkpoints: tuple[str, ...] = REQUIRED_CHECKPOINTS) -> dict[str, Any]:
    """Record schema, key, and receipt differences without stopping the release."""
    errors: list[str] = []

    def note(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    event_keys = [row["event_key"] for row in events]
    note(len(event_keys) == len(set(event_keys)), "Duplicate event_key")
    plan_keys = [(row["event_key"], row["checkpoint"]) for row in plan]
    note(len(plan_keys) == len(set(plan_keys)), "Duplicate observation plan key")
    expected = {(event, checkpoint) for event in event_keys for checkpoint in checkpoints}
    note(set(plan_keys) == expected, "Observation plan does not cover every required checkpoint")
    ledger_keys = {(row["event_key"], row["checkpoint"]) for row in ledger}
    note(ledger_keys == expected, "Coverage ledger does not preserve event denominator")
    note(all(row["final_state"] in TERMINAL_LEDGER_STATES for row in ledger), "Non-terminal coverage state")
    note(all(row["event_key"] in set(event_keys) for row in plan + attempts + snapshots + fields + ledger), "Foreign key violation")
    attempt_ids = {row["request_id"] for row in attempts}
    note(all(row["request_id"] in attempt_ids for row in snapshots + fields + ledger if row["request_id"]), "Unknown request_id")
    body_map: dict[str, bytes] = {}
    for row in bodies:
        try:
            body_map[str(row["request_id"])] = base64.b64decode(str(row["body_b64"]))
        except (KeyError, ValueError) as exc:
            errors.append(f"Unreadable response body for {row.get('request_id', '')}: {exc}")
    for snapshot in snapshots:
        note(snapshot["request_id"] in body_map, f"Missing response body for {snapshot['request_id']}")
        if snapshot["request_id"] in body_map:
            note(sha256_bytes(body_map[snapshot["request_id"]]) == snapshot["response_sha256"], f"Response hash mismatch: {snapshot['request_id']}")
    for row in fields:
        note(row["field_name"] in ALLOWED_FIELD_NAMES, f"Unexpected field: {row['field_name']}")
        note(row["presence"] in ("present", "absent"), "Unknown field presence")
        if row["presence"] == "present":
            note(bool(row["value_hash"]), "Present field has no value hash")
    return {"status": "PASS" if not errors else "RECORDED", "errors": errors, "event_count": len(events), "plan_rows": len(plan), "attempt_rows": len(attempts), "snapshot_rows": len(snapshots), "field_rows": len(fields), "ledger_rows": len(ledger), "final_state_counts": dict(sorted(Counter(row["final_state"] for row in ledger).items())), "rules": ["unique event key", "event x checkpoint denominator", "foreign keys", "response SHA-256", "field allow-list", "terminal ledger state"]}


def _copy_release_file(source: Path, release_dir: Path, published: list[dict[str, Any]]) -> None:
    target = release_dir / source.name
    target.write_bytes(source.read_bytes())
    published.append({"path": target.name, "bytes": target.stat().st_size, "sha256": sha256_file(target)})


def build_release(run_dir: str | Path, release_dir: str | Path, protocol: dict[str, Any], source_register: list[dict[str, str]]) -> dict[str, Any]:
    """Write release files even when recorded validation differences remain."""
    run = Path(run_dir)
    release = Path(release_dir)
    release.mkdir(parents=True, exist_ok=True)
    events = read_csv(run / "launch_events.csv", EVENT_FIELDS)
    plan = read_csv(run / "observation_plan.csv", PLAN_FIELDS)
    attempts = read_csv(run / "request_attempts.csv", ATTEMPT_FIELDS)
    snapshots = read_csv(run / "response_snapshots.csv", SNAPSHOT_FIELDS)
    bodies = read_jsonl(run / "response_bodies.jsonl")
    fields = parse_field_observations(events, snapshots, bodies, protocol)
    ledger = build_coverage_ledger(plan, attempts, snapshots, fields)
    checkpoints = tuple(protocol.get("observation", {}).get("checkpoints") or REQUIRED_CHECKPOINTS)
    validation = validate_release(events, plan, attempts, snapshots, fields, ledger, bodies, checkpoints)
    validation["timing_state_counts"] = dict(sorted(Counter(row["timing_state"] for row in ledger).items()))
    missing_transactions = run / "missing_transactions.jsonl"
    if missing_transactions.exists():
        unresolved = read_jsonl(missing_transactions)
        if unresolved:
            validation["errors"].append(f"{len(unresolved)} chain transactions could not be fetched after the configured retries")
            validation["status"] = "RECORDED"
    cohort_status = run / "cohort_status.json"
    if cohort_status.exists():
        status = json.loads(cohort_status.read_text())
        if status.get("phase") != "finished" or not status.get("final_scan_complete", False):
            validation["status"] = "INCOMPLETE"
            validation["errors"].append("Live sampling or checkpoints are incomplete; see cohort_status.json and run_status.json")
        if status.get("signature_failures"):
            validation["status"] = "INCOMPLETE"
            validation["errors"].append(f"{len(status['signature_failures'])} chain transactions remain unresolved; the event denominator may be incomplete")
    write_csv(run / "field_observations.csv", FIELD_FIELDS, fields)
    write_csv(run / "coverage_ledger.csv", LEDGER_FIELDS, ledger)
    write_json(run / "validation_results.json", {**validation, "protocol_hash": stable_hash(protocol), "created_at": now_utc()})
    published: list[dict[str, Any]] = []
    for filename in ("launch_events.csv", "observation_plan.csv", "request_attempts.csv", "request_attempts.jsonl", "response_snapshots.csv", "field_observations.csv", "coverage_ledger.csv", "validation_results.json"):
        _copy_release_file(run / filename, release, published)
    selection_file = run.parent / "API_SELECTION.json"
    selection = read_json(selection_file) if selection_file.exists() else {}
    selected_rpc = selection if selection.get("status") == "direct_collection" else None
    selected_endpoint = (selected_rpc or {}).get("selected_endpoint")
    manifest = {"release_version": str(protocol["protocol_version"]), "created_at": now_utc(), "protocol_sha256": stable_hash(protocol), "files": published,
                "selected_rpc": selected_rpc,
                "source_listing_scope": "Source eligibility; selected_rpc pins this invocation's chosen RPC. Metadata network requests and policy refusals are recorded in request_attempts.csv.",
                "exact_replay": [{"source": row["host"], "reproduction_mode": row.get("reproduction_mode", "excluded_from_exact_replay")} for row in source_register if row.get("reproduction_mode") in ("immutable_requery", "archived_replay")],
                "procedural_rerun": [{"source": row["host"], "reproduction_mode": row.get("reproduction_mode", "live_rerun_only")} for row in source_register if row.get("reproduction_mode") == "live_rerun_only" and (row.get("acquisition_method") != "Solana JSON-RPC" or row.get("endpoint") == selected_endpoint)],
                "raw_response_policy": "Raw third-party responses are run artifacts and are excluded from release unless source rights explicitly allow redistribution."}
    write_json(release / "release_manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--release-dir", required=True)
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--source-register", required=True)
    args = parser.parse_args()
    build_release(args.run_dir, args.release_dir, read_json(args.protocol), read_csv(args.source_register))


if __name__ == "__main__":
    main()
