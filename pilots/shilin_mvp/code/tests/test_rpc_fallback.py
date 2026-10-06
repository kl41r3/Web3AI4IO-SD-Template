"""RPC route behavior and creation-role regression checks."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mvp.acquire_chain import FallbackRpcClient, JsonRpcClient, decode_transaction, PUMPFUN_PROGRAM_ID
from mvp.common import read_json, read_csv

ROOT = Path(__file__).resolve().parents[1]


class RpcFallbackTests(unittest.TestCase):
    def setUp(self):
        self.protocol = read_json(ROOT / "configs/mvp_protocol.json")
        self.sources = read_csv(ROOT / "configs/source_register.csv")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.output = Path(self.tmp.name)
        self.visited = []

    def factory(self, failures):
        visited = self.visited

        def make(route):
            class Client:
                calls = 0
                opener = None

                def call(self, method, params):
                    self.calls += 1
                    visited.append((route["candidate_id"], method))
                    if (route["candidate_id"], method) in failures:
                        raise RuntimeError("Connection failed")
                    return {"result": 123}
            return Client()
        return make

    def client(self, failures=(), endpoint=None):
        return FallbackRpcClient(self.protocol, endpoint, self.output, self.sources, self.factory(failures))

    def test_working_original_route_keeps_the_original_connection(self):
        client = self.client()
        self.assertEqual(client.call("getSlot", []), {"result": 123})
        client.call("getTransaction", ["tx"])
        self.assertEqual(self.visited, [("publicnode-solana-mainnet", "getSlot"), ("publicnode-solana-mainnet", "getTransaction")])
        receipt = read_json(self.output / "API_SELECTION.json")
        self.assertEqual(receipt["transport"], "https")
        self.assertEqual(receipt["getTransaction_pause_seconds"], 0.2)

    def test_https_failure_uses_http3_on_the_same_endpoint(self):
        client = self.client({("publicnode-solana-mainnet", "getSlot")})
        client.call("getSlot", [])
        self.assertEqual([row[0] for row in self.visited], ["publicnode-solana-mainnet", "publicnode-solana-mainnet-http3"])
        receipt = read_json(self.output / "API_SELECTION.json")
        self.assertEqual(receipt["selected_endpoint"], "https://solana-rpc.publicnode.com")
        self.assertEqual(receipt["transport"], "http3")
        self.assertEqual(receipt["getTransaction_pause_seconds"], 0.2)
        self.assertEqual([row["status"] for row in receipt["connection_history"]], ["failed", "selected"])

    def test_both_publicnode_routes_fail_before_official_rpc_is_used(self):
        client = self.client({("publicnode-solana-mainnet", "getTransaction"), ("publicnode-solana-mainnet-http3", "getTransaction")})
        client.call("getTransaction", ["tx"])
        receipt = read_json(self.output / "API_SELECTION.json")
        self.assertEqual([row[0] for row in self.visited], ["publicnode-solana-mainnet", "publicnode-solana-mainnet-http3", "solana-official-rpc-secondary-region"])
        self.assertEqual(receipt["getTransaction_pause_seconds"], 3.0)
        self.assertEqual(receipt["selected_endpoint"], "https://api.mainnet-beta.solana.com")
        self.assertEqual(client.calls, 3)
        row = next(row for row in read_csv(self.output / "source_register.csv") if row["host"] == "api.mainnet-beta.solana.com")
        self.assertEqual(row["automatic_access_decision"], "allowed")

    def test_route_can_change_after_a_successful_slot_query(self):
        client = self.client({("publicnode-solana-mainnet", "getTransaction")})
        client.call("getSlot", [])
        client.call("getTransaction", ["tx"])
        receipt = read_json(self.output / "API_SELECTION.json")
        self.assertEqual([row["status"] for row in receipt["connection_history"]], ["selected", "failed", "selected"])
        self.assertEqual(receipt["transport"], "http3")

    def test_every_route_failure_is_recorded_and_reported(self):
        client = self.client({(row["candidate_id"], "getSlot") for row in self.protocol["api_candidates"]})
        with self.assertRaisesRegex(RuntimeError, "Connection failed"):
            client.call("getSlot", [])
        rows = [json.loads(line) for line in (self.output / "rpc_connection_attempts.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["status"] == "failed" for row in rows))

    def test_explicit_official_endpoint_uses_its_three_second_pace(self):
        client = self.client(endpoint="https://api.mainnet-beta.solana.com")
        client.call("getTransaction", ["tx"])
        self.assertEqual(len(self.visited), 1)
        self.assertEqual(read_json(self.output / "API_SELECTION.json")["getTransaction_pause_seconds"], 3)

    def test_resume_retries_original_order_and_keeps_prior_receipts(self):
        failures = {(row["candidate_id"], "getSlot") for row in self.protocol["api_candidates"][:2]}
        self.client(failures).call("getSlot", [])
        self.client().call("getSlot", [])
        receipt = read_json(self.output / "API_SELECTION.json")
        self.assertEqual(receipt["selected_candidate"], "publicnode-solana-mainnet")
        self.assertEqual(len(receipt["connection_history"]), 4)

    def test_old_saved_protocol_gets_the_current_connection_order(self):
        self.protocol["api_candidates"] = [self.protocol["api_candidates"][0], self.protocol["api_candidates"][2]]
        self.protocol["api_candidates"][1]["access_decision"] = "not_used"
        client = self.client({("publicnode-solana-mainnet", "getSlot"), ("publicnode-solana-mainnet-http3", "getSlot")})
        client.call("getSlot", [])
        self.assertEqual(read_json(self.output / "API_SELECTION.json")["selected_endpoint"], "https://api.mainnet-beta.solana.com")

    def test_real_clients_receive_route_pacing(self):
        from io import BytesIO
        class Opener:
            def open(self, request, timeout):
                return BytesIO(b'{"result": {}}')
        for endpoint, interval in [("https://solana-rpc.publicnode.com", 0.2), ("https://api.mainnet-beta.solana.com", 3.0)]:
            def factory(route):
                return JsonRpcClient(route["endpoint"], opener=Opener(), method_min_intervals=route.get("rpc_method_min_interval_seconds"))
            client = FallbackRpcClient(self.protocol, endpoint, client_factory=factory)
            with patch("mvp.acquire_chain.time.sleep") as sleep:
                client.call("getTransaction", ["tx"])
            sleep.assert_called_once_with(interval)


class CreatorDecoderTests(unittest.TestCase):
    def transaction(self, discriminator, creator=True):
        alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        raw = discriminator
        for text in ("Example", "EX", "https://example.test/metadata.json"):
            value = text.encode()
            raw += len(value).to_bytes(4, "little") + value
        if creator:
            raw += bytes([1]) * 32
        number = int.from_bytes(raw, "big")
        encoded = ""
        while number:
            number, remainder = divmod(number, 58)
            encoded = alphabet[remainder] + encoded
        accounts = ["MintExample"] + ["Account"] * 6 + ["TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"]
        return {"result": {"slot": 1, "meta": {"err": None}, "transaction": {"message": {"accountKeys": [], "instructions": [{"programId": PUMPFUN_PROGRAM_ID, "data": encoded, "accounts": accounts}]}}}}

    def test_create_and_create_v2_read_the_creator_argument(self):
        for discriminator in (bytes((24, 30, 200, 40, 5, 28, 7, 119)), bytes((214, 144, 76, 236, 95, 139, 49, 180))):
            events = decode_transaction(self.transaction(discriminator), "signature", 1)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["creator"], "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi")
            self.assertEqual(events[0]["mint"], "MintExample")

    def test_other_instructions_with_string_arguments_are_not_creates(self):
        self.assertEqual(decode_transaction(self.transaction(b"12345678"), "signature", 1), [])

    def test_missing_creator_argument_is_not_published_as_a_valid_create(self):
        self.assertEqual(decode_transaction(self.transaction(bytes((214, 144, 76, 236, 95, 139, 49, 180)), creator=False), "signature", 1), [])


if __name__ == "__main__":
    unittest.main()
