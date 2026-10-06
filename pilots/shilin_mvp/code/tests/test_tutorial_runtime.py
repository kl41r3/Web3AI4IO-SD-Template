import json
import sys
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mvp.common import read_json, read_csv
from mvp.live_cohort import discover_window_events
from mvp.tutorial import prepare_collection, schedule
from run_mvp import finalize_incomplete, selected_rpc_candidate


class TutorialRuntimeTests(unittest.TestCase):
    def test_partial_release_is_not_pass_and_can_be_resumed(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dataset"
            prepared = prepare_collection(ROOT, mode="rpc", enrollment_hours=1 / 60, checkpoint_hours=[1 / 60, 2 / 60], output_dir=str(output), max_runtime_minutes=3)
            protocol = read_json(prepared.protocol_path)
            sources = read_csv(output.parent / f"{output.name}-source-register.csv")
            finalize_incomplete(output, protocol, sources, "runtime_limit")
            result = read_json(output / "mvp_release/validation_results.json")
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertTrue(result["errors"])
            again = prepare_collection(ROOT, mode="rpc", output_dir=str(output), max_runtime_minutes=3)
            self.assertTrue(again.resumed)
            self.assertEqual(again.offsets, prepared.offsets)

    def test_transaction_batch_yields_and_continues_from_last_signature(self):
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["acquisition"].update(page_limit=5, transactions_per_poll=2)
        calls = []

        class Client:
            def call(self, method, params):
                calls.append((method, params))
                if method == "getSignaturesForAddress":
                    page = [{"signature": f"sig-{i}", "blockTime": 4000 - i, "err": None} for i in range(5)]
                    if params[1].get("before"):
                        i = int(params[1]["before"].split("-")[1])
                        page = page[i + 1:]
                    return {"result": page}
                return {"result": None}

        state = {}
        start = datetime.fromtimestamp(1000, timezone.utc)
        end = datetime.fromtimestamp(5000, timezone.utc)
        discover_window_events(Client(), protocol, state, start, end)
        self.assertFalse(state["_scan_complete"])
        self.assertEqual(state["pending_before"], "sig-1")
        self.assertEqual(sum(m == "getTransaction" for m, _ in calls), 2)
        calls.clear()
        discover_window_events(Client(), protocol, state, start, end)
        self.assertEqual(calls[0][1][1]["before"], "sig-1")
        self.assertEqual(state["pending_before"], "sig-3")

    def test_retry_batch_can_exhaust_budget_before_a_new_page(self):
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["acquisition"]["transactions_per_poll"] = 1
        calls = []

        class Client:
            def call(self, method, params):
                calls.append(method)
                return {"result": None}

        state = {"signature_failures": {"retry-me": {"count": "1", "block_time": "3000"}}, "pending_before": "cursor"}
        start = datetime.fromtimestamp(1000, timezone.utc)
        end = datetime.fromtimestamp(5000, timezone.utc)
        discover_window_events(Client(), protocol, state, start, end)
        self.assertEqual(calls, ["getTransaction"])
        self.assertEqual(state["pending_before"], "cursor")
        self.assertTrue(state["_batch_yielded"])
        self.assertFalse(state["_scan_complete"])

    def test_rpc_provenance_names_the_selected_endpoint(self):
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        official = "https://api.mainnet-beta.solana.com"
        self.assertEqual(selected_rpc_candidate(protocol, official)["candidate_id"], "solana-official-rpc-secondary-region")
        self.assertEqual(selected_rpc_candidate(protocol, "https://rpc.example.test")["candidate_id"], "user-selected-rpc")

    def test_explicit_rpc_choice_updates_source_protocol_and_manifest(self):
        from mvp.common import write_json
        from mvp.build_release import build_release
        endpoint = "https://api.mainnet-beta.solana.com"
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dataset"
            prepared = prepare_collection(ROOT, mode="rpc", rpc_endpoint=endpoint, output_dir=str(output))
            protocol = read_json(prepared.protocol_path)
            candidate = next(row for row in protocol["api_candidates"] if row["endpoint"] == endpoint)
            self.assertEqual(candidate["access_decision"], "allowed")
            self.assertEqual(candidate["role"], "user-selected-chain-rpc")
            sources = read_csv(output.parent / f"{output.name}-source-register.csv")
            source = next(row for row in sources if row["endpoint"] == endpoint)
            self.assertEqual(source["temporary_retention_decision"], "retain run receipt only")
            self.assertEqual(source["reproduction_mode"], "live_rerun_only")
            self.assertEqual(source["derived_field_publication_decision"], "exclude")
            self.assertEqual(source["checked_at"], datetime.now(timezone.utc).date().isoformat())
            finalize_incomplete(output, protocol, sources, "runtime_limit")
            write_json(output / "API_SELECTION.json", {"status": "direct_collection", "selected_endpoint": endpoint})
            manifest = build_release(output / "run", output / "mvp_release", protocol, sources)
            rpc_hosts = [row["source"] for row in manifest["procedural_rerun"] if "solana" in row["source"]]
            self.assertEqual(rpc_hosts, ["api.mainnet-beta.solana.com"])
            self.assertEqual(manifest["selected_rpc"]["selected_endpoint"], endpoint)

    def test_denied_origin_is_recorded_before_gateway_without_origin_request(self):
        from mvp.acquire_chain import load_fixture
        from mvp.observe_metadata import FetchResult, observe_events, build_observation_plan
        from mvp.live_cohort import _terminal_keys
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        sources = read_csv(ROOT / "configs/source_register.csv")
        event = load_fixture(ROOT / "fixtures/sample_events.json")[0]
        event["metadata_uri"] = "https://ipfs.io/ipfs/bafytest"
        plan = build_observation_plan([event], protocol)[:1]
        called = []
        def transport(url):
            called.append(url)
            return FetchResult(status="success", http_status="200", body=b'{}', content_type="application/json")
        with tempfile.TemporaryDirectory() as tmp:
            attempts, _ = observe_events([event], plan, sources, protocol, tmp, transport=transport)
        self.assertEqual(called, ["https://pump.mypinata.cloud/ipfs/bafytest"])
        self.assertEqual([row["status"] for row in attempts], ["route_refused_policy", "success"])
        self.assertEqual(attempts[0]["completed_at"], "")
        self.assertEqual(attempts[0]["error_class"], "source_register_denied")
        self.assertEqual(attempts[0]["uri"], event["metadata_uri"])
        self.assertEqual(_terminal_keys([attempts[0]], {}, 5), set())

    def test_observation_retry_attempt_numbers_continue_across_calls(self):
        from mvp.acquire_chain import load_fixture
        from mvp.observe_metadata import FetchResult, observe_events, build_observation_plan
        from mvp.build_release import build_coverage_ledger, parse_field_observations
        from mvp.common import read_jsonl
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["observation"]["max_attempts"] = 1
        sources = read_csv(ROOT / "configs/source_register.csv")
        event = load_fixture(ROOT / "fixtures/sample_events.json")[0]
        plan = build_observation_plan([event], protocol)[:1]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            observe_events([event], plan, sources, protocol, output, transport=lambda _: FetchResult(status="timeout"), clock=lambda: "2026-09-28T00:10:00Z")
            attempts, snapshots = observe_events([event], plan, sources, protocol, output, transport=lambda _: FetchResult(status="success", http_status="200", body=b'{}'), clock=lambda: "2026-09-28T00:11:00Z", append=True)
            self.assertEqual([row["attempt"] for row in attempts], ["1", "2"])
            fields = parse_field_observations([event], snapshots, read_jsonl(output / "response_bodies.jsonl"), protocol)
            ledger = build_coverage_ledger(plan, attempts, snapshots, fields)
            self.assertEqual(ledger[0]["final_state"], "observed")

    def test_rate_backoff_survives_other_methods_and_honors_retry_after(self):
        from unittest.mock import Mock, MagicMock, patch, call
        from urllib.error import HTTPError
        from mvp.acquire_chain import JsonRpcClient
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"result": []}'
        opener = Mock()
        opener.open.side_effect = [HTTPError("https://rpc.example.test", 429, "limit", {"Retry-After": "10"}, None), response,
                                   HTTPError("https://rpc.example.test", 429, "limit", {}, None), response]
        client = JsonRpcClient("https://rpc.example.test", opener=opener, min_interval=0, max_attempts=1)
        with patch("mvp.acquire_chain.time.sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "429"):
                client.call("getTransaction", [])
            client.call("getSignaturesForAddress", [])
            with self.assertRaisesRegex(RuntimeError, "429"):
                client.call("getTransaction", [])
            self.assertEqual(sleep.call_args_list, [call(10.0), call(2.0)])
            client.call("getTransaction", [])
        self.assertNotIn("getTransaction", client.rate_delays)

    def test_selected_endpoint_pacing_keeps_other_methods_at_default(self):
        from unittest.mock import Mock, MagicMock, patch, call
        from mvp.acquire_chain import JsonRpcClient, rpc_method_intervals
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        official = "https://api.mainnet-beta.solana.com"
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"result": []}'
        opener = Mock()
        opener.open.return_value = response
        intervals = rpc_method_intervals(protocol, official)
        self.assertEqual(intervals, {"getTransaction": 3.0})
        self.assertEqual(rpc_method_intervals(protocol, "https://solana-rpc.publicnode.com"), {})
        self.assertEqual(rpc_method_intervals(protocol, "https://rpc.example.test"), {})
        official_client = JsonRpcClient(official, opener=opener, method_min_intervals=intervals)
        default_client = JsonRpcClient("https://solana-rpc.publicnode.com", opener=opener)
        with patch("mvp.acquire_chain.time.sleep") as sleep:
            official_client.call("getTransaction", [])
            official_client.call("getSignaturesForAddress", [])
            default_client.call("getTransaction", [])
            official_client.call("getTransaction", [])
        self.assertEqual(sleep.call_args_list, [call(3.0), call(0.2), call(0.2), call(3.0)])

    def test_configured_pacing_preserves_retry_after_and_failed_transaction_backoff(self):
        from unittest.mock import Mock, MagicMock, patch, call
        from urllib.error import HTTPError
        from mvp.acquire_chain import JsonRpcClient
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"result": []}'
        opener = Mock()
        opener.open.side_effect = [HTTPError("https://rpc.example.test", 429, "limit", {"Retry-After": "10"}, None), response,
                                   HTTPError("https://rpc.example.test", 429, "limit", {}, None), response]
        client = JsonRpcClient("https://rpc.example.test", opener=opener, min_interval=0, max_attempts=1,
                               method_min_intervals={"getTransaction": 3.0})
        with patch("mvp.acquire_chain.time.sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "429"):
                client.call("getTransaction", [])
            client.call("getSignaturesForAddress", [])
            with self.assertRaisesRegex(RuntimeError, "429"):
                client.call("getTransaction", [])
            client.call("getTransaction", [])
        self.assertEqual(sleep.call_args_list, [call(10.0), call(3.0), call(3.0)])
        self.assertNotIn("getTransaction", client.rate_delays)

    def test_live_client_receives_selected_endpoint_pacing(self):
        from unittest.mock import patch
        from mvp.common import write_json
        from mvp.live_cohort import run_live_cohort
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["observation"].update(checkpoint_offsets_seconds={"T0": 0, "T+1s": 1}, enrollment_seconds=1)
        sources = read_csv(ROOT / "configs/source_register.csv")
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "run"
            write_json(output / "cohort_clock.json", {"collection_started_at": "2026-09-29T00:00:00Z", "enrollment_until": "2026-09-29T00:00:01Z"})
            with patch("mvp.live_cohort.FallbackRpcClient") as make_client:
                make_client.return_value.call.return_value = {"result": []}
                run_live_cohort(protocol, sources, output, rpc_endpoint="https://api.mainnet-beta.solana.com",
                                clock=lambda: "2026-09-29T00:00:02Z", sleep=lambda _: self.fail("scan should finish"))
            self.assertEqual(make_client.call_args.args[1], "https://api.mainnet-beta.solana.com")
            self.assertEqual(make_client.call_args.args[2], output.parent)

    def test_unresolved_transaction_prevents_pass_after_scan_finishes(self):
        from mvp.common import write_json
        from mvp.build_release import build_release
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dataset"
            protocol = read_json(ROOT / "configs/mvp_protocol.json")
            sources = read_csv(ROOT / "configs/source_register.csv")
            finalize_incomplete(output, protocol, sources, "runtime_limit")
            write_json(output / "run/cohort_status.json", {"phase": "finished", "final_scan_complete": True, "signature_failures": {"unfetched": {"count": "1"}}})
            build_release(output / "run", output / "mvp_release", protocol, sources)
            result = read_json(output / "mvp_release/validation_results.json")
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertTrue(any("denominator" in error for error in result["errors"]))

    def test_transactions_are_retried_after_signature_scan_completion(self):
        from mvp.common import write_json
        from mvp.live_cohort import run_live_cohort
        from unittest.mock import Mock
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["observation"].update(checkpoint_offsets_seconds={"T0": 0, "T+1s": 1}, enrollment_seconds=1)
        sources = read_csv(ROOT / "configs/source_register.csv")
        def discover(_client, _protocol, state, _start, _end):
            state["signature_failures"] = {}
            state["_scan_complete"] = True
            return []
        checked = Mock(side_effect=discover)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "run"
            output.mkdir()
            write_json(output / "cohort_clock.json", {"collection_started_at": "2026-09-29T00:00:00Z", "enrollment_until": "2026-09-29T00:00:01Z"})
            write_json(output / "cohort_status.json", {"phase": "interrupted", "final_scan_complete": True, "signature_failures": {"unfetched": {"count": "1"}}})
            run_live_cohort(protocol, sources, output, clock=lambda: "2026-09-29T00:00:02Z", sleep=lambda _: self.fail("should finish after retry"), discover=checked)
            checked.assert_called_once()
            self.assertEqual(read_json(output / "cohort_status.json")["signature_failures"], {})

    def test_interrupted_cohort_resume_preserves_clock_and_t0_attempt(self):
        from datetime import timedelta
        from mvp.acquire_chain import load_fixture
        from mvp.live_cohort import run_live_cohort
        from mvp.observe_metadata import FixtureTransport
        protocol = read_json(ROOT / "configs/mvp_protocol.json")
        protocol["observation"].update(checkpoint_offsets_seconds={"T0": 0, "T+4s": 4, "T+5s": 5}, enrollment_seconds=4, poll_seconds=1)
        sources = read_csv(ROOT / "configs/source_register.csv")
        event = load_fixture(ROOT / "fixtures/sample_events.json")[0]
        transport = FixtureTransport({uri: ROOT / path for uri, path in protocol["fixture"]["uri_files"].items()})

        class Clock:
            now = datetime(2026, 9, 29, tzinfo=timezone.utc)

            def __call__(self):
                return self.now.isoformat().replace("+00:00", "Z")

            def sleep(self, seconds):
                self.now += timedelta(seconds=seconds)

        clock = Clock()
        def stop(_seconds):
            raise KeyboardInterrupt
        def discover(*_args):
            return [event]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "run"
            with self.assertRaises(KeyboardInterrupt):
                run_live_cohort(protocol, sources, output, transport=transport, clock=clock, sleep=stop, discover=discover)
            original_clock = (output / "cohort_clock.json").read_bytes()
            first_attempt = read_csv(output / "request_attempts.csv")[0]
            clock.now += timedelta(seconds=3)
            events, _ = run_live_cohort(protocol, sources, output, transport=transport, clock=clock, sleep=clock.sleep, discover=discover)
            self.assertEqual((output / "cohort_clock.json").read_bytes(), original_clock)
            self.assertEqual(len(events), 1)
            self.assertEqual(len(read_csv(output / "observation_plan.csv")), 3)
            attempts = read_csv(output / "request_attempts.csv")
            self.assertEqual(len(attempts), 3)
            self.assertEqual(attempts[0], first_attempt)
            self.assertEqual(read_json(output / "cohort_status.json")["phase"], "finished")

    def test_future_checkpoint_rule_and_runtime_budget_validation(self):
        with self.assertRaisesRegex(ValueError, "at or after"):
            schedule(1, [0.5, 1])
        for value in [-1, True, float("nan")]:
            with self.assertRaisesRegex(ValueError, "MAX_RUNTIME_MINUTES"):
                prepare_collection(ROOT, mode="rpc", max_runtime_minutes=value)

    def test_runtime_limit_is_a_supervisor_process_not_a_stream_timeout(self):
        from unittest.mock import patch
        import subprocess
        import run_mvp
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "dataset"
            argv = ["run_mvp.py", "--mode", "rpc", "--output", str(output), "--max-runtime-seconds", "1"]
            with patch.object(sys, "argv", argv), patch.object(run_mvp.subprocess, "Popen") as launch:
                launch.return_value.wait.side_effect = [subprocess.TimeoutExpired("worker", 1), None]
                run_mvp.main()
                launch.return_value.kill.assert_called_once()
            self.assertEqual(read_json(output / "run_status.json")["stop_reason"], "runtime_limit")
            self.assertEqual(read_json(output / "mvp_release/validation_results.json")["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
