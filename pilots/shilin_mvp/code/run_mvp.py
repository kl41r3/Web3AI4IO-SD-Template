#!/usr/bin/env python3
"""Run the Pump.fun MVP data flow.

Fixture mode replays local files. RPC mode enrolls a rolling cohort from the
actual collection start, then waits through the protocol's observation
checkpoints. Network calls use IPv4. Failures are saved and retried.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from mvp.acquire_chain import acquire_events, benchmark_candidates, FallbackRpcClient
from mvp.build_release import build_release
from mvp.common import parse_utc, read_csv, read_json, sha256_file, stable_hash, write_csv, write_json
from mvp.gateway_preflight import confirm_metadata_gateways
from mvp.live_cohort import run_live_cohort
from mvp.observe_metadata import FixtureTransport, build_observation_plan, observe_events
from mvp.schema import PLAN_FIELDS, EVENT_FIELDS, ATTEMPT_FIELDS, SNAPSHOT_FIELDS
from mvp.tutorial import record_rpc_override


def finalize_incomplete(output: Path, protocol: dict, sources: list[dict], reason: str) -> None:
    """Retain partial results after a bounded worker stops; never label them PASS."""
    run = output / "run"
    run.mkdir(parents=True, exist_ok=True)
    for name, fields in (("launch_events.csv", EVENT_FIELDS), ("observation_plan.csv", PLAN_FIELDS), ("request_attempts.csv", ATTEMPT_FIELDS), ("response_snapshots.csv", SNAPSHOT_FIELDS)):
        if not (run / name).exists():
            write_csv(run / name, fields, [])
    for name in ("request_attempts.jsonl", "response_bodies.jsonl"):
        if not (run / name).exists():
            (run / name).write_text("")
    saved = run / "cohort_protocol.json"
    if saved.exists():
        protocol = read_json(saved)
    write_json(output / "mvp_protocol.json", protocol)
    if not (output / "source_register.csv").exists():
        write_csv(output / "source_register.csv", list(sources[0]), sources)
    status_path = run / "cohort_status.json"
    status = read_json(status_path) if status_path.exists() else {}
    status.update(phase="interrupted", stop_reason=reason, interrupted_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    write_json(status_path, status)
    build_release(run, output / "mvp_release", protocol, sources)
    write_json(output / "run_status.json", {"phase": "interrupted", "mode": "rpc", "stop_reason": reason, "validation": "INCOMPLETE"})
    print(f"INCOMPLETE: {reason}. Retained dataset: {output}", flush=True)


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT.parent / "data"


def selected_rpc_candidate(protocol: dict, endpoint: str) -> dict:
    """Name the endpoint actually selected, including a user-supplied URL."""
    return next((row for row in protocol["api_candidates"] if row.get("endpoint") == endpoint),
                {"candidate_id": "user-selected-rpc", "endpoint": endpoint})


def unfinished_output(path: Path) -> Path | None:
    """Return the newest unfinished cohort under ``path``, if one exists."""
    if not path.exists():
        return None
    candidates = []
    if (path / "run" / "cohort_status.json").exists():
        candidates.append(path)
    for child in path.iterdir():
        if child.is_dir() and (child / "run" / "cohort_status.json").exists():
            candidates.append(child)
    unfinished = []
    for item in candidates:
        try:
            status = read_json(item / "run" / "cohort_status.json")
        except (OSError, ValueError):
            continue
        if status.get("phase") != "finished":
            unfinished.append(item)
    if not unfinished:
        return None
    return max(unfinished, key=lambda item: (item / "run" / "cohort_status.json").stat().st_mtime)


def resolve_output(path: Path, resume: bool) -> Path:
    """Resume an unfinished cohort, or start a new timestamped run."""
    if resume:
        found = unfinished_output(path)
        if found is not None:
            print(f"resuming unfinished cohort: {found}", flush=True)
            return found
        if (path / "run").is_dir() and not (path / "run" / "cohort_clock.json").exists():
            return path
    if path.exists() and any(path.iterdir()):
        stamp = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
        return path / stamp
    return path


def confirm_gateways_with_retry(protocol: dict, output: Path) -> list[dict]:
    """Try the metadata gateways more than once, then continue either way."""
    settings = protocol.get("metadata_gateways") or {}
    attempts = max(1, int(settings.get("preflight_attempts", 5)))
    delay = float(settings.get("preflight_sleep_seconds", 15))
    passed: list[dict] = []
    for attempt in range(1, attempts + 1):
        try:
            passed = confirm_metadata_gateways(protocol, output)
        except Exception as exc:
            print(f"gateway preflight attempt {attempt} failed: {exc.__class__.__name__}: {exc}", flush=True)
            passed = []
        if passed:
            print(f"gateway preflight passed: {', '.join(str(row.get('host', '')) for row in passed)}", flush=True)
            return passed
        if attempt < attempts:
            print(f"gateway preflight empty; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
    print("gateway preflight did not pass; collection continues with declared URIs and registered routes", flush=True)
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fixture", "rpc"), default="fixture")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--rpc-endpoint")
    parser.add_argument("--protocol", default=str(ROOT / "configs" / "mvp_protocol.json"))
    parser.add_argument("--source-register", default=str(ROOT / "configs" / "source_register.csv"))
    parser.add_argument("--fixture", default=str(ROOT / "fixtures" / "sample_events.json"))
    parser.add_argument("--start-at-utc", default="", help="Do not start a new live cohort before this UTC time; the saved cohort clock records the actual start.")
    parser.add_argument("--max-runtime-seconds", type=int, default=0, help="Live invocation limit; zero uses the remaining checkpoint time plus five minutes. Includes preflight and a scheduled-start wait.")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    protocol = read_json(args.protocol)
    source_register = read_csv(args.source_register)
    if args.mode == "rpc" and args.rpc_endpoint:
        record_rpc_override(protocol, source_register, args.rpc_endpoint)
    if args.max_runtime_seconds < 0:
        parser.error("--max-runtime-seconds must be nonnegative")
    if args.mode == "rpc" and not args.worker:
        target = resolve_output(Path(args.output), resume=True)
        budget = args.max_runtime_seconds
        if not budget:
            seconds = max(protocol["observation"]["checkpoint_offsets_seconds"].values())
            clock_file = target / "run/cohort_clock.json"
            if clock_file.exists():
                elapsed = (datetime.now(timezone.utc) - parse_utc(read_json(clock_file)["collection_started_at"])).total_seconds()
                seconds = max(0, seconds - elapsed)
            if args.start_at_utc:
                seconds += max(0, (parse_utc(args.start_at_utc) - datetime.now(timezone.utc)).total_seconds())
            budget = int(seconds) + 300
        command = [sys.executable, "-u", str(Path(__file__).resolve()), "--worker", "--mode", "rpc", "--output", str(target), "--protocol", args.protocol, "--source-register", args.source_register, "--fixture", args.fixture]
        if args.rpc_endpoint:
            command.extend(["--rpc-endpoint", args.rpc_endpoint])
        if args.start_at_utc:
            command.extend(["--start-at-utc", args.start_at_utc])
        print(f"Live invocation limit: {budget}s; dataset: {target}", flush=True)
        worker = subprocess.Popen(command, cwd=ROOT)
        try:
            code = worker.wait(timeout=budget)
            if code:
                finalize_incomplete(target, protocol, source_register, f"worker_exit_{code}")
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait()
            finalize_incomplete(target, protocol, source_register, "runtime_limit")
        except KeyboardInterrupt:
            worker.terminate()
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()
            finalize_incomplete(target, protocol, source_register, "user_interrupt")
        return
    output = resolve_output(Path(args.output), resume=args.mode == "rpc")
    run_dir = output / "run"
    release_dir = output / "mvp_release"
    resumed = args.mode == "rpc" and (run_dir / "cohort_clock.json").exists()
    if resumed:
        saved_protocol_path = run_dir / "cohort_protocol.json"
        if not saved_protocol_path.exists():
            raise SystemExit(f"Cannot resume without saved cohort protocol: {saved_protocol_path}")
        protocol = read_json(saved_protocol_path)
    if args.mode == "rpc" and args.rpc_endpoint:
        record_rpc_override(protocol, source_register, args.rpc_endpoint)
    run_dir.mkdir(parents=True, exist_ok=True)
    release_dir.mkdir(parents=True, exist_ok=True)
    source_copy = output / "source_register.csv"
    write_csv(source_copy, list(source_register[0]), source_register)

    benchmark_path = output / "api_benchmark.csv"
    candidates = [row for row in protocol["api_candidates"] if row.get("access_decision") == "allowed"] or list(protocol["api_candidates"])
    if args.mode == "fixture":
        benchmark_candidates(protocol, candidates, benchmark_path, args.fixture)
        benchmark_rows = read_csv(benchmark_path)
        winner = next((row for row in benchmark_rows if row.get("status") == "ok"), None)
        selection_status = "fixture_only"
    else:
        rpc_client = FallbackRpcClient(protocol, args.rpc_endpoint, output, source_register)
        rpc_client.call("getSlot", [{"commitment": "finalized"}])
    observation = protocol["observation"]
    checkpoint_names = ", ".join(observation["checkpoints"])
    enrollment_hours = int(observation["enrollment_seconds"]) / 3600
    if args.mode == "fixture":
        selection = {"status": selection_status, "selected_candidate": winner.get("candidate_id") if winner else None,
                     "selected_endpoint": None, "evidence_file": "api_benchmark.csv", "evidence_row": 2 if winner else None}
        write_json(output / "API_SELECTION.json", selection)

    if args.mode == "fixture":
        events = acquire_events(protocol, run_dir, args.fixture, args.rpc_endpoint)
        plan = build_observation_plan(events, protocol)
        write_csv(run_dir / "observation_plan.csv", PLAN_FIELDS, plan)
        uri_files = {uri: ROOT / relative for uri, relative in protocol["fixture"]["uri_files"].items()}
        observe_events(events, plan, source_register, protocol, run_dir, transport=FixtureTransport(uri_files))
        shutil.copyfile(args.protocol, output / "mvp_protocol.json")
    else:
        if not resumed:
            passed = confirm_gateways_with_retry(protocol, output)
            protocol.setdefault("metadata_gateways", {})["confirmed"] = passed
            if args.start_at_utc:
                target = parse_utc(args.start_at_utc)
                while True:
                    remaining = (target - datetime.now(timezone.utc)).total_seconds()
                    if remaining <= 0:
                        break
                    print(f"waiting {remaining:.0f}s for selected start", flush=True)
                    time.sleep(min(remaining, 30))
        events, protocol = run_live_cohort(protocol, source_register, run_dir, args.rpc_endpoint, rpc_client=rpc_client)
        rpc_client.close()
        write_json(output / "mvp_protocol.json", protocol)
    manifest = build_release(run_dir, release_dir, protocol, source_register)

    write_csv(output / "source_register.csv", list(source_register[0]), source_register)
    write_json(output / "input_manifest.json", {"protocol_sha256": stable_hash(protocol), "source_register_sha256": sha256_file(args.source_register), "mode": args.mode, "fixture_sha256": sha256_file(args.fixture) if args.mode == "fixture" else None, "release_manifest": str((release_dir / "release_manifest.json").relative_to(output))})
    write_csv(output / "DGP_DATA_LINEAGE.csv", ["lineage_id", "input", "output", "rule", "code_location", "validation", "responsible", "version"], [
        {"lineage_id": "DGP-ACQ-001", "input": "RPC/fixture transaction", "output": "launch_events.event_key", "rule": f"Successful Pump.fun create. Live mode enrolls from collection start for {enrollment_hours:g} hours; fixture mode uses the protocol window.", "code_location": "mvp/live_cohort.py:run_live_cohort", "validation": "validate_release", "responsible": "Owen", "version": protocol["protocol_version"]},
        {"lineage_id": "DGP-OBS-001", "input": "collection_started_at or block_time", "output": "observation_plan.scheduled_at", "rule": f"Live T0 is collection start; checkpoints are {checkpoint_names}. Fixture mode uses block_time.", "code_location": "mvp/live_cohort.py:run_live_cohort", "validation": "plan denominator check", "responsible": "Owen", "version": protocol["protocol_version"]},
        {"lineage_id": "DGP-OBS-002", "input": "metadata_uri", "output": "request_attempts.request_url", "rule": "Public http(s) URI is requested on IPv4. ipfs:// and ar:// keep the original URI and record the gateway route. Local and private hosts are not collected. Failures are retried.", "code_location": "mvp/observe_metadata.py:observe_events", "validation": "coverage ledger terminal state", "responsible": "Owen", "version": protocol["protocol_version"]},
        {"lineage_id": "DGP-REL-001", "input": "JSON response", "output": "field_observations", "rule": "fixed JSON pointers and presence/type", "code_location": "mvp/build_release.py:parse_field_observations", "validation": "field allow-list and response hash", "responsible": "Owen", "version": protocol["protocol_version"]},
    ])
    write_json(output / "run_status.json", {"phase": "finished", "mode": args.mode, "events": len(events), "release_files": len(manifest["files"]), "address_family": "ipv4"})
    print(f"Run complete: {output}")
    print(f"Events: {len(events)}; release files: {len(manifest['files'])}")


if __name__ == "__main__":
    main()
