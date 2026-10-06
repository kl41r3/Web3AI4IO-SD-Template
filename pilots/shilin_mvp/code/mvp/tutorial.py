"""Small, testable setup layer for the collection tutorial notebook."""

from __future__ import annotations

import json
import sys
import csv
import ipaddress
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Collection:
    code_dir: Path
    output: Path
    protocol_path: Path
    command: tuple[str, ...]
    mode: str
    enrollment_seconds: int
    offsets: dict[str, int]
    start_at_utc: str
    resumed: bool
    max_runtime_seconds: int


def _seconds(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive number of hours")
    try:
        hours = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be a positive number of hours") from exc
    if not hours.is_finite() or hours <= 0:
        raise ValueError(f"{label} must be a positive number of hours")
    seconds = int((hours * 3600).to_integral_value(rounding=ROUND_HALF_UP))
    if seconds < 1:
        raise ValueError(f"{label} must be at least one second")
    return seconds


def _label(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"T+{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"T+{seconds // 60}m"
    return f"T+{seconds}s"


def schedule(enrollment_hours: object, checkpoint_hours: object) -> tuple[int, dict[str, int]]:
    enrollment = _seconds(enrollment_hours, "ENROLLMENT_HOURS")
    if not isinstance(checkpoint_hours, (list, tuple)) or not checkpoint_hours:
        raise ValueError("CHECKPOINT_HOURS must be a nonempty list, for example [24, 60]")
    values = [_seconds(value, "CHECKPOINT_HOURS entry") for value in checkpoint_hours]
    if values != sorted(set(values)):
        raise ValueError("CHECKPOINT_HOURS must be increasing and contain no duplicate times")
    if enrollment not in values:
        raise ValueError("CHECKPOINT_HOURS must include the enrollment end")
    if any(value < enrollment for value in values):
        raise ValueError("Each checkpoint must be at or after the enrollment end")
    return enrollment, {"T0": 0, **{_label(value): value for value in values}}


def _start_time(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if not value.endswith("Z"):
        raise ValueError("START_AT_UTC must be a UTC time ending in Z")
    try:
        target = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("START_AT_UTC must look like 2026-10-03T00:00:00Z") from exc
    if target <= datetime.now(timezone.utc):
        raise ValueError("START_AT_UTC is in the past; live enrollment cannot be reconstructed retroactively")
    return target.isoformat(timespec="seconds").replace("+00:00", "Z")


def record_rpc_override(protocol: dict, sources: list[dict], endpoint: str) -> None:
    """Record a selected runtime route without changing publication decisions."""
    for candidate in protocol["api_candidates"]:
        if candidate.get("endpoint") == endpoint:
            candidate.update(access_decision="allowed", role="user-selected-chain-rpc", selection_note="Explicit endpoint choice for this invocation; availability is established only by its request evidence.")
    source = next((row for row in sources if row.get("endpoint") == endpoint), None)
    if source is None:
        source = {field: "" for field in sources[0]}
        source.update(host=urlsplit(endpoint).hostname or "", endpoint=endpoint, acquisition_method="Solana JSON-RPC")
        sources.append(source)
    source.update(automatic_access_decision="allowed", query_scope="User-selected Solana RPC for this cohort", temporary_retention_decision="retain run receipt only", reproduction_mode="live_rerun_only", checked_at=datetime.now(timezone.utc).date().isoformat(), evidence_id="SRC-RPC-USER")


def prepare_collection(
    code_dir: str | Path,
    *,
    mode: str = "rpc",
    start_at_utc: str = "",
    enrollment_hours: object = 24,
    checkpoint_hours: object = (24, 60),
    output_dir: str = "",
    rpc_endpoint: str = "",
    max_runtime_minutes: object = 0,
) -> Collection:
    code = Path(code_dir).expanduser().resolve()
    template = code / "configs" / "mvp_protocol.json"
    if not (code / "run_mvp.py").is_file() or not template.is_file():
        raise FileNotFoundError(f"Collector code is missing from {code}")
    if mode not in {"rpc", "fixture"}:
        raise ValueError("MODE must be 'rpc' or 'fixture'")
    try:
        budget = Decimal(str(max_runtime_minutes))
    except InvalidOperation as exc:
        raise ValueError("MAX_RUNTIME_MINUTES must be zero (automatic) or a positive number") from exc
    if isinstance(max_runtime_minutes, bool) or not budget.is_finite() or budget < 0:
        raise ValueError("MAX_RUNTIME_MINUTES must be zero (automatic) or a positive number")
    budget_seconds = int((budget * 60).to_integral_value(rounding=ROUND_HALF_UP))
    if budget > 0 and budget_seconds < 1:
        raise ValueError("MAX_RUNTIME_MINUTES must be at least one second")
    if rpc_endpoint and mode != "rpc":
        raise ValueError("RPC_ENDPOINT is only used in rpc mode")
    if start_at_utc.strip() and mode != "rpc":
        raise ValueError("START_AT_UTC is only used in rpc mode")
    if rpc_endpoint:
        parsed_endpoint = urlsplit(rpc_endpoint)
        if parsed_endpoint.scheme != "https" or not parsed_endpoint.hostname or parsed_endpoint.username or parsed_endpoint.password or parsed_endpoint.fragment:
            raise ValueError("RPC_ENDPOINT must be a public HTTPS URL without credentials or a fragment")
        host = parsed_endpoint.hostname.lower().rstrip(".")
        if host in {"localhost", "localhost.localdomain"} or host.endswith((".localhost", ".local")):
            raise ValueError("RPC_ENDPOINT cannot be a local host")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if not address.is_global:
                raise ValueError("RPC_ENDPOINT cannot be a private or reserved address")
    requested_output = output_dir.strip()
    if requested_output:
        output = Path(requested_output).expanduser().resolve()
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        prefix = "live" if mode == "rpc" else "fixture"
        base = code.parent / "pumpfun_data"
        output = base / f"{prefix}-{stamp}"
        suffix = 2
        while output.exists():
            output = base / f"{prefix}-{stamp}-{suffix}"
            suffix += 1

    status_path = output / "run" / "cohort_status.json"
    clock_path = output / "run" / "cohort_clock.json"
    sidecar = output.parent / f"{output.name}-protocol.json"
    source_sidecar = output.parent / f"{output.name}-source-register.csv"
    run_status = output / "run_status.json"
    if (run_status.exists() and json.loads(run_status.read_text()).get("phase") == "finished") or (status_path.exists() and json.loads(status_path.read_text()).get("phase") == "finished"):
        raise ValueError(f"Dataset is complete; choose a new OUTPUT_DIR: {output}")
    resumed = mode == "rpc" and (status_path.exists() or clock_path.exists() or (output / "run").is_dir())
    if output.exists() and any(output.iterdir()) and not resumed:
        raise ValueError(f"OUTPUT_DIR already contains data; choose a new folder: {output}")
    if mode == "fixture" and resumed:
        raise ValueError("Fixture runs are not resumed; choose a new OUTPUT_DIR")

    if resumed:
        protocol_path = output / "run" / "cohort_protocol.json" if clock_path.exists() else sidecar
        if not protocol_path.is_file():
            raise FileNotFoundError(f"Saved protocol needed to resume is missing: {protocol_path}")
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        saved_rpc_endpoint = str(protocol.get("requested_rpc_endpoint") or "")
        if rpc_endpoint and rpc_endpoint != saved_rpc_endpoint:
            raise ValueError("RPC_ENDPOINT differs from the interrupted run")
        rpc_endpoint = saved_rpc_endpoint or rpc_endpoint
        start = "" if clock_path.exists() else str(protocol.get("requested_start_at_utc") or "")
    else:
        enrollment, offsets = schedule(enrollment_hours, checkpoint_hours)
        start = _start_time(start_at_utc) if mode == "rpc" else ""
        protocol = json.loads(template.read_text(encoding="utf-8"))
        protocol["observation"]["checkpoints"] = list(offsets)
        protocol["observation"]["checkpoint_offsets_seconds"] = offsets
        protocol["observation"]["enrollment_seconds"] = enrollment
        protocol["requested_start_at_utc"] = start
        protocol["requested_rpc_endpoint"] = rpc_endpoint
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        protocol_path = sidecar

        with (code / "configs" / "source_register.csv").open(newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            fields = list(reader.fieldnames or [])
            sources = list(reader)
        if rpc_endpoint:
            record_rpc_override(protocol, sources, rpc_endpoint)
        sidecar.write_text(json.dumps(protocol, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        with source_sidecar.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sources)

    observation = protocol["observation"]
    offsets = {str(k): int(v) for k, v in observation["checkpoint_offsets_seconds"].items()}
    enrollment = int(observation["enrollment_seconds"])
    if not source_sidecar.is_file():
        source_sidecar = output / "source_register.csv"
    if not source_sidecar.is_file():
        raise FileNotFoundError(f"Saved source register needed to run or resume is missing: {source_sidecar}")
    command = [sys.executable, "-u", "run_mvp.py", "--mode", mode, "--protocol", str(protocol_path), "--source-register", str(source_sidecar), "--output", str(output)]
    if mode == "rpc":
        if not budget_seconds:
            remaining = max(offsets.values())
            if clock_path.exists():
                saved_clock = json.loads(clock_path.read_text())
                started = datetime.fromisoformat(saved_clock["collection_started_at"].replace("Z", "+00:00"))
                remaining = max(0, remaining - int((datetime.now(timezone.utc) - started).total_seconds()))
            waiting = max(0, int((datetime.fromisoformat(start.replace("Z", "+00:00")) - datetime.now(timezone.utc)).total_seconds())) if start else 0
            budget_seconds = remaining + waiting + 300
        command.extend(["--max-runtime-seconds", str(budget_seconds)])
    if rpc_endpoint:
        command.extend(["--rpc-endpoint", rpc_endpoint])
    if start:
        command.extend(["--start-at-utc", start])
    return Collection(code, output, protocol_path, tuple(command), mode, enrollment, offsets, start, resumed, budget_seconds)


def summarize_collection(output_dir: str | Path) -> dict:
    output = Path(output_dir)
    path = output / "mvp_release" / "validation_results.json"
    if not path.is_file():
        raise FileNotFoundError(f"Collection did not create a release validation file: {path}")
    result = json.loads(path.read_text(encoding="utf-8"))
    return {
        "dataset": str(output),
        "validation": result.get("status"),
        "events": result.get("event_count"),
        "planned_observations": result.get("plan_rows"),
        "successful_responses": result.get("snapshot_rows"),
        "final_states": result.get("final_state_counts"),
        "validation_errors": result.get("errors", []),
        "run_phase": json.loads((output / "run_status.json").read_text()).get("phase") if (output / "run_status.json").exists() else "unknown",
        "timing_states": result.get("timing_state_counts", {}),
        "rpc_connection": json.loads((output / "API_SELECTION.json").read_text()) if (output / "API_SELECTION.json").exists() else {},
    }
