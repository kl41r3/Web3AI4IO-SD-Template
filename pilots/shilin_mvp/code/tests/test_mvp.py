import base64
import csv
import json
import tempfile
import unittest
from pathlib import Path

import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mvp.acquire_chain import decode_create_args, load_fixture
from mvp.build_release import build_coverage_ledger, parse_field_observations, validate_release
from mvp.common import read_csv, read_json, read_jsonl, write_csv
from mvp.observe_metadata import FetchResult, FixtureTransport, build_observation_plan, metadata_request_candidates, observe_events
from mvp.schema import ATTEMPT_FIELDS, EVENT_FIELDS, FIELD_FIELDS, LEDGER_FIELDS, PLAN_FIELDS, SNAPSHOT_FIELDS


class MvpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.protocol = read_json(ROOT / "configs/mvp_protocol.json")
        self.sources = read_csv(ROOT / "configs/source_register.csv")
        self.events = load_fixture(ROOT / "fixtures/sample_events.json")
        self.plan = build_observation_plan(self.events, self.protocol)

    def test_fixture_event_keys_and_denominator(self):
        self.assertEqual(len(self.events), 4)
        self.assertEqual(len({row["event_key"] for row in self.events}), 4)
        self.assertEqual(len(self.plan), 12)
        self.assertEqual({row["checkpoint"] for row in self.plan}, {"T0", "T+24h", "T+60h"})

    def test_borsh_decoder(self):
        def borsh(*values):
            raw = bytes((24, 30, 200, 40, 5, 28, 7, 119))
            for value in values:
                data = value.encode()
                raw += len(data).to_bytes(4, "little") + data
            raw += bytes(range(32))
            alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
            number = int.from_bytes(raw, "big")
            encoded = ""
            while number:
                number, rem = divmod(number, 58)
                encoded = alphabet[rem] + encoded
            return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + encoded
        self.assertEqual(decode_create_args(borsh("Cumin", "CUM", "https://x.test/m.json")), ("Cumin", "CUM", "https://x.test/m.json"))

    def test_anchor_uses_collection_start(self):
        anchored = build_observation_plan(self.events, self.protocol, anchor_time="2026-09-29T10:00:00Z")
        self.assertEqual({row["scheduled_at"] for row in anchored if row["checkpoint"] == "T0"}, {"2026-09-29T10:00:00Z"})
        self.assertEqual({row["scheduled_at"] for row in anchored if row["checkpoint"] == "T+24h"}, {"2026-09-30T10:00:00Z"})
        self.assertEqual({row["scheduled_at"] for row in anchored if row["checkpoint"] == "T+60h"}, {"2026-10-01T22:00:00Z"})

    def test_declared_ipfs_and_arweave_routes_are_explicit(self):
        ipfs = metadata_request_candidates("ipfs://bafyfixture/metadata.json", self.protocol)
        self.assertEqual(ipfs[0], ("https://pump.mypinata.cloud/ipfs/bafyfixture/metadata.json", "pump_pinata_gateway"))
        self.assertEqual(ipfs[1], ("https://gateway.pinata.cloud/ipfs/bafyfixture/metadata.json", "pinata_gateway"))
        self.assertEqual(metadata_request_candidates("ar://transaction-id", self.protocol), [("https://arweave.net/transaction-id", "arweave_gateway")])

    def test_unknown_host_policy_preserves_event_denominator(self):
        mapping = {uri: ROOT / path for uri, path in self.protocol["fixture"]["uri_files"].items()}
        out = self.work / "run"
        observe_events(self.events, self.plan, self.sources, self.protocol, out, FixtureTransport(mapping), clock=lambda: "2026-09-28T01:00:00Z")
        attempts = read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)
        refused = [row for row in attempts if row["event_key"].endswith("fixture-signature-003:0:0")]
        self.assertEqual(len(refused), 3)
        self.assertTrue(all(row["status"] == "http_error" and row["http_status"] == "404" for row in refused))

    def test_release_validation_and_field_missingness(self):
        mapping = {uri: ROOT / path for uri, path in self.protocol["fixture"]["uri_files"].items()}
        out = self.work / "run"
        observe_events(self.events, self.plan, self.sources, self.protocol, out, FixtureTransport(mapping), clock=lambda: "2026-09-28T01:00:00Z")
        attempts = read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)
        snapshots = read_csv(out / "response_snapshots.csv", SNAPSHOT_FIELDS)
        bodies = read_jsonl(out / "response_bodies.jsonl")
        fields = parse_field_observations(self.events, snapshots, bodies, self.protocol)
        ledger = build_coverage_ledger(self.plan, attempts, snapshots, fields)
        result = validate_release(self.events, self.plan, attempts, snapshots, fields, ledger, bodies)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(ledger), 12)
        self.assertTrue(any(row["field_name"] == "telegram" and row["presence"] == "present" and row["value_type"] == "null" for row in fields))
        self.assertEqual(sum(row["final_state"] == "request_failed" for row in ledger), 3)

    def test_json_hash_tamper_is_detected(self):
        mapping = {uri: ROOT / path for uri, path in self.protocol["fixture"]["uri_files"].items()}
        out = self.work / "run"
        observe_events(self.events, self.plan, self.sources, self.protocol, out, FixtureTransport(mapping), clock=lambda: "2026-09-28T01:00:00Z")
        snapshots = read_csv(out / "response_snapshots.csv", SNAPSHOT_FIELDS)
        bodies = read_jsonl(out / "response_bodies.jsonl")
        attempts = read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)
        fields = parse_field_observations(self.events, snapshots, bodies, self.protocol)
        ledger = build_coverage_ledger(self.plan, attempts, snapshots, fields)
        bodies[0]["body_b64"] = base64.b64encode(b"tampered").decode()
        result = validate_release(self.events, self.plan, attempts, snapshots, fields, ledger, bodies)
        self.assertEqual(result["status"], "RECORDED")
        self.assertTrue(any("Response hash mismatch" in item for item in result["errors"]))

    def test_live_cohort_waits_for_collection_clock(self):
        from datetime import datetime, timedelta, timezone

        from mvp.build_release import build_release
        from mvp.live_cohort import run_live_cohort
        from mvp.observe_metadata import FixtureTransport

        protocol = json.loads(json.dumps(self.protocol))
        protocol["observation"]["checkpoint_offsets_seconds"] = {"T0": 0, "T+24h": 2, "T+60h": 5}
        protocol["observation"]["enrollment_seconds"] = 4
        protocol["observation"]["poll_seconds"] = 1
        start = datetime(2026, 9, 29, tzinfo=timezone.utc)

        class Clock:
            def __init__(self):
                self.now = start

            def __call__(self):
                return self.now.isoformat().replace("+00:00", "Z")

            def sleep(self, seconds):
                self.now += timedelta(seconds=seconds)

        clock = Clock()
        calls = {"n": 0}
        event = dict(self.events[0])

        def discover(client, protocol_arg, state, window_start, window_end):
            calls["n"] += 1
            return [event] if calls["n"] == 1 else []

        mapping = {uri: ROOT / path for uri, path in protocol["fixture"]["uri_files"].items()}
        out = self.work / "live"
        events, frozen = run_live_cohort(protocol, self.sources, out, transport=FixtureTransport(mapping), clock=clock, sleep=clock.sleep, discover=discover)
        self.assertEqual(len(events), 1)
        self.assertEqual(frozen["window"]["clock"], "collection_start")
        build_release(out, self.work / "live-release", frozen, self.sources)
        ledger = read_csv(out / "coverage_ledger.csv", LEDGER_FIELDS)
        self.assertEqual(sorted(row["checkpoint"] for row in ledger), ["T+24h", "T+60h", "T0"])
        self.assertTrue(all(row["final_state"] == "observed" for row in ledger))
        discover_calls = calls["n"]
        again, _frozen_again = run_live_cohort(protocol, self.sources, out, transport=FixtureTransport(mapping), clock=clock, sleep=clock.sleep, discover=discover)
        self.assertEqual(len(again), 1)
        self.assertEqual(calls["n"], discover_calls)
        started = {row["checkpoint"]: row["started_at"] for row in read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)}
        self.assertTrue(started["T0"].startswith("2026-09-29T00:00:00"))
        self.assertTrue(started["T+24h"].startswith("2026-09-29T00:00:02"))
        self.assertTrue(started["T+60h"].startswith("2026-09-29T00:00:05"))

    def test_failed_declared_ipfs_uri_uses_confirmed_gateway(self):
        from mvp.observe_metadata import FetchResult

        protocol = json.loads(json.dumps(self.protocol))
        protocol["observation"]["max_attempts"] = 1
        protocol["metadata_gateways"] = {"confirmed": [{"host": "gateway.pinata.cloud", "template": "https://gateway.pinata.cloud/ipfs/{cid}", "status": "json_ok"}]}
        event = dict(self.events[0])
        event["metadata_uri"] = "https://ipfs.io/ipfs/bafypreflight"
        plan = [dict(self.plan[0])]
        plan[0]["event_key"] = event["event_key"]
        declared = "https://ipfs.io/ipfs/bafypreflight"
        gateway = "https://gateway.pinata.cloud/ipfs/bafypreflight"
        body = (ROOT / "fixtures/metadata/token-001.json").read_bytes()

        def transport(uri: str) -> FetchResult:
            if uri == gateway:
                return FetchResult(status="success", http_status="200", body=body, content_type="application/json")
            return FetchResult(status="timeout", error_class="timeout", error_message="timed out")

        out = self.work / "gateway"
        sources = [dict(row) for row in self.sources]
        next(row for row in sources if row["host"] == "ipfs.io")["automatic_access_decision"] = "allowed"
        observe_events([event], plan, sources, protocol, out, transport=transport, clock=lambda: "2026-09-28T01:00:00Z")
        attempts = read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)
        self.assertEqual([row["route_id"] for row in attempts], ["declared_exact", "pinata_gateway"])
        self.assertEqual(attempts[0]["status"], "timeout")
        self.assertEqual(attempts[1]["status"], "success")
        self.assertEqual(attempts[1]["uri"], declared)
        self.assertEqual(attempts[1]["request_url"], gateway)
        snapshots = read_csv(out / "response_snapshots.csv", SNAPSHOT_FIELDS)
        self.assertEqual(snapshots[0]["uri"], declared)
        self.assertEqual(snapshots[0]["request_url"], gateway)
        self.assertEqual(snapshots[0]["parse_state"], "json_ok")

    def test_private_host_is_refused_without_stopping(self):
        event = dict(self.events[0], metadata_uri="http://127.0.0.1/secret.json")
        plan = [dict(self.plan[0], event_key=event["event_key"])]
        out = self.work / "private"
        observe_events([event], plan, self.sources, self.protocol, out, transport=lambda uri: FetchResult(status="success", http_status="200", body=b"{}"), clock=lambda: "2026-09-28T01:00:00Z")
        attempts = read_csv(out / "request_attempts.csv", ATTEMPT_FIELDS)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "not_collected_policy")
        self.assertEqual(attempts[0]["error_class"], "local_or_private_host")

    def test_discovery_failure_is_retried(self):
        from datetime import datetime, timedelta, timezone

        from mvp.live_cohort import run_live_cohort

        protocol = json.loads(json.dumps(self.protocol))
        protocol["observation"]["checkpoint_offsets_seconds"] = {"T0": 0, "T+24h": 2, "T+60h": 4}
        protocol["observation"]["enrollment_seconds"] = 3
        protocol["observation"]["poll_seconds"] = 1
        protocol["observation"]["max_retry_rounds"] = 2
        start = datetime(2026, 9, 29, tzinfo=timezone.utc)

        class Clock:
            def __init__(self):
                self.now = start

            def __call__(self):
                return self.now.isoformat().replace("+00:00", "Z")

            def sleep(self, seconds):
                self.now += timedelta(seconds=seconds)

        clock = Clock()
        calls = {"n": 0}
        event = dict(self.events[0])

        def discover(client, protocol_arg, state, window_start, window_end):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("rpc down")
            return [event] if calls["n"] == 2 else []

        out = self.work / "retry"
        mapping = {uri: ROOT / path for uri, path in protocol["fixture"]["uri_files"].items()}
        events, _frozen = run_live_cohort(protocol, self.sources, out, transport=FixtureTransport(mapping), clock=clock, sleep=clock.sleep, discover=discover)
        self.assertEqual(len(events), 1)
        self.assertGreaterEqual(calls["n"], 2)
        status = read_json(out / "cohort_status.json")
        self.assertEqual(status["phase"], "finished")
        self.assertTrue((out / "launch_events.csv").exists())

    def test_signature_pages_resume_after_the_last_completed_page(self):
        from mvp.live_cohort import discover_window_events

        protocol = json.loads(json.dumps(self.protocol))
        protocol["acquisition"]["page_limit"] = 2
        protocol["acquisition"]["max_pages"] = 1
        pages = {
            None: [
                {"signature": "sig-a", "blockTime": 4000, "err": None},
                {"signature": "sig-b", "blockTime": 3000, "err": None},
            ],
            "sig-b": [
                {"signature": "sig-c", "blockTime": 2000, "err": None},
                {"signature": "sig-d", "blockTime": 1500, "err": None},
            ],
            "sig-d": [
                {"signature": "sig-e", "blockTime": 500, "err": None},
            ],
        }
        requested = []

        class Client:
            def call(self, method, params):
                requested.append((method, params[1].get("before") if method == "getSignaturesForAddress" else params[0]))
                if method == "getSignaturesForAddress":
                    return {"result": pages[params[1].get("before")]}
                return {"result": None}

        state: dict = {"cursor": None, "signature_failures": {}, "page_done": []}
        window_start = __import__("datetime").datetime.fromtimestamp(1000, tz=__import__("datetime").timezone.utc)
        window_end = __import__("datetime").datetime.fromtimestamp(5000, tz=__import__("datetime").timezone.utc)
        discover_window_events(Client(), protocol, state, window_start, window_end)
        self.assertEqual(state["pending_before"], "sig-b")
        self.assertEqual(state["scan_tip"], "sig-a")
        self.assertIsNone(state["cursor"])
        discover_window_events(Client(), protocol, state, window_start, window_end)
        self.assertEqual(state["pending_before"], "sig-d")
        self.assertEqual(state["cursor"], None)
        discover_window_events(Client(), protocol, state, window_start, window_end)
        self.assertIsNone(state["pending_before"])
        self.assertEqual(state["cursor"], "sig-a")
        signature_calls = [item for item in requested if item[0] == "getSignaturesForAddress"]
        self.assertEqual([item[1] for item in signature_calls], [None, "sig-b", "sig-d"])


if __name__ == "__main__":
    unittest.main()
