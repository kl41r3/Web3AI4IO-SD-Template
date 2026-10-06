# Pump.fun MVP implementation

This folder combines the v2 metadata routes with the DKUCC collection path. Fixture mode uses the Python standard library. Live mode also uses `curl_cffi==0.16.3` for the HTTP/3 connection.

- `mvp/acquire_chain.py` decodes Pump.fun create instructions and talks to Solana JSON-RPC over IPv4. Failed calls are retried inside the client.
- `mvp/live_cohort.py` enrolls creates for 24 hours from the moment the process starts. That moment is T0. The same process then collects T+24h and, after 36 more hours, T+60h. The saved clock fixes the enrollment window for recovery.
- `mvp/observe_metadata.py` requests the declared URI first, keeps that URI in every receipt, and records `request_url` plus `route_id` for IPFS and Arweave gateways. Public hosts are allowed. Local and private hosts are refused.
- `mvp/gateway_preflight.py` checks metadata gateways before a live run. A failed check is retried, then collection continues.
- `mvp/build_release.py` parses fixed JSON pointers, writes the coverage ledger, records validation differences, and writes the release.

The default `fixture` mode is offline and repeats the same sample events and metadata. With the default schedule it produces 4 events, 12 planned observations, 9 successful snapshots and 72 field rows (72 present, 0 absent), with 3 intentional request failures and validation `PASS`. Request timestamps and release-file hashes vary between runs. `rpc` mode follows the connection order below and records the service actually used.

## Run

From the repository root, enter the code directory once. Use Python 3.12 for the tutorial environment:

```bash
cd pilots/shilin_mvp/code
python3.12 run_mvp.py --mode fixture --output output/fixture-demo
python3.12 -m unittest discover -s tests -v
```

Live collection is one process. It enrolls Pump.fun creates for 24 hours from the moment it starts, treats that moment as T0, requests metadata as each create appears, waits, then collects the same cohort at T+24h and again 36 hours later at T+60h. After the last checkpoint it parses fields, writes the coverage ledger, and publishes `mvp_release/`. The collector runs the scheduled checks automatically. The run needs about 60 hours:

```bash
python3.12 -m pip install -r requirements.txt
python3.12 run_mvp.py --mode rpc --output output/live
```

Receipts are rewritten as events and metadata attempts arrive. Add `--max-runtime-seconds 180` to limit one live invocation to three minutes; the default `0` allows time through the final checkpoint plus five minutes. A time limit or interruption preserves the dataset and reports unfinished work as `INCOMPLETE` in `run_status.json`. If `run/cohort_status.json` is unfinished, repeat the same command to resume its saved clock, scan position, schedule and metadata attempts. Keep the full output folder and its original endpoint setting.

## Live connection order

Omit `--rpc-endpoint` to use the automatic connection order below. An RPC is a service that reads Solana transactions for the collector. HTTP/3 is another way to connect to the same PublicNode service. The collector tries each connection up to twice, then tries the next row. It keeps a working connection and switches again if later transaction queries fail.

| Order | Service and connection | Wait after each successful transaction query |
|---|---|---|
| 1 | PublicNode using Shilin's original HTTPS client | 0.2 seconds |
| 2 | The same PublicNode URL using HTTP/3 | 0.2 seconds |
| 3 | Official Solana RPC using HTTPS | 3 seconds |

The official RPC's 3-second wait is 15 times the PublicNode wait. It reduces query frequency after the earlier rate-limit response, so scanning takes longer. Each request also takes time to travel over the network. If a service asks the collector to wait before retrying (`Retry-After`), the collector follows that wait. The console prints the selected service and interval; `API_SELECTION.json` and `rpc_connection_attempts.jsonl` save the connection choices.

You can enter a public HTTPS RPC URL to choose one service directly. Entering the PublicNode URL keeps the automatic three-row order. Resume with the same `--rpc-endpoint` setting; connection attempts start with the saved order and retain their earlier records.

The creation decoder checks the official `create` and `create_v2` instruction identifiers and reads the creator from the instruction arguments, after the name, symbol and metadata URI. The creator field supports grouping tokens by their designated creator address.


A short connectivity check that does not start the 60-hour wait:

```bash
python3.12 -m mvp.smoke_rpc --output output/smoke
```
