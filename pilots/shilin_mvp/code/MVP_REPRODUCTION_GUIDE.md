# MVP reproduction guide

1. Use Python 3.12 for the tutorial environment and clone/copy this `code` directory; run the commands below from it. For live collection, install the connection dependency with `python -m pip install -r requirements.txt`.
2. Run `python -m unittest discover -s tests -v`.
3. Run `python run_mvp.py --mode fixture --output output/reproduction-1`.
4. Check `mvp_release/validation_results.json`: with the default schedule, expect 4 events, 12 planned observations, 9 successful snapshots, 72 field rows (72 present, 0 absent), 3 intentional request failures and `PASS`. Verify each release file against the size and SHA-256 recorded in its own `release_manifest.json`. The fixture source is labeled `archived_replay`: fixed response-body hashes match across runs, while request timestamps and release-file hashes vary.
5. For a real rerun, use a source-register endpoint, retain all receipts, and label the result `live_rerun_only`. Compare the same procedure and recorded source choices across runs; live responses and timestamps may change.

Every file has a stable schema. The release contains no third-party raw response body; local `run/response_bodies.jsonl` is an execution artifact used to construct field observations.


Live connection order: PublicNode HTTPS, the same PublicNode endpoint over HTTP/3, then the official Solana RPC. PublicNode transaction queries wait 0.2 seconds after success; official-RPC transaction queries wait 3 seconds. Connection failures and switches are saved in `rpc_connection_attempts.jsonl`, and `API_SELECTION.json` records the selected route and interval.
