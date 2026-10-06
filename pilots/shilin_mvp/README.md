# Pump.fun v3 cohort

This folder contains the code and the completed live dataset for one Pump.fun metadata cohort. The run enrolled newly created tokens for 24 hours and then observed the same tokens again after the collection window and after 36 further hours.

Use [Pumpfun_v3_Collection_Tutorial.ipynb](Pumpfun_v3_Collection_Tutorial.ipynb) to configure and launch live collection with the adjacent `code/` directory. The dataset and results below describe the original September 2026 collection.

The cohort is a feasibility collection with three checkpoints, anchored to the collection start. The formal study plan describes seven days and four checkpoints.

## What was done

One process started on the DKUCC common CPU partition at `2026-09-29T06:16:11Z`. That moment is T0. From then until `2026-09-30T06:16:11Z`, the process scanned the Pump.fun program for successful token-creation transactions and requested each declared metadata URI as the event was enrolled. The same cohort was requested again at `2026-09-30T06:16:11Z` (T+24h, the end of collection) and at `2026-10-01T18:16:11Z` (T+60h, 36 hours after collection ended). The job finished at `2026-10-01T21:33:14Z`.

Every enrolled creation stays in the denominator. A failed metadata request is recorded; it does not remove the event. The release validation status is `PASS`.

| Item | Result |
|---|---|
| Network and program | Solana mainnet, Pump.fun `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| Enrolled creation events | 1,248 |
| Required observations | 3,744 (1,248 events × T0, T+24h, T+60h) |
| Metadata attempts | 11,539 |
| Successfully parsed observations | 3,216 |
| Request failures | 330 |
| Refused by collection policy | 198 |
| Parsed JSON fields | `name`, `symbol`, `description`, `createdOn`, `image`, `website`, `twitter`, `telegram` |

Checkpoint coverage among the 1,248 events: T0 parsed 1,077 responses, T+24h parsed 1,070, and T+60h parsed 1,069.

The collector records the public metadata fields listed above. Raw response bodies remain in the local run directory.

## Where the data came from

Chain events came from one public Solana JSON-RPC endpoint:

- `https://solana-rpc.publicnode.com` (PublicNode). The client connects over IPv4 and calls `getSignaturesForAddress` and `getTransaction`.

`https://api.mainnet-beta.solana.com` is listed in the source register and was not used. Its DNS answers did not connect from the DKUCC common CPU partition.

Metadata came from the URI written in the Pump.fun create instruction. The collector requests that declared URL first. If the declaration is an IPFS CID and the first request fails, it tries the same CID through these registered gateways:

- `https://pump.mypinata.cloud`
- `https://gateway.pinata.cloud`

In this run, most successful requests used the declared host directly. The largest declared hosts were `gateway.irys.xyz` and `pump.mypinata.cloud`, followed by smaller public HTTPS hosts named in individual creation transactions. `ipfs.io` and `arweave.net` are registered as unused because IPv4 connections from this partition timed out. Local and private hosts are refused before any request.

The source register, terms URLs, and access decisions for this collection are in `data/live-v3-20260929T061604Z/source_register.csv`. The frozen protocol is `data/live-v3-20260929T061604Z/mvp_protocol.json`.

## What is in this folder

| Path | Contents |
|---|---|
| `code/` | Python collector with HTTP/3 fallback, metadata observer, release builder, fixtures, and tests |
| `data/live-v3-20260929T061604Z/mvp_release/` | Published tables: events, observation plan, attempts, snapshots, fields, coverage ledger, and validation |
| `data/live-v3-20260929T061604Z/run/` | Receipts written during the run, including response bodies |

The offline fixture mode checks the pipeline and does not contact the network:

```bash
cd code
python3 run_mvp.py --mode fixture --output /tmp/pumpfun-v3-fixture
python3 -m unittest discover -s tests -v
```

Live collection uses the same entry point with `--mode rpc`. A live run waits through the enrollment and later checkpoints. The stored September dataset can be read directly.

## Connections for a new run

Install live dependencies with `python -m pip install -r code/requirements.txt`. A new run tries PublicNode with the original HTTPS client first, the same endpoint over HTTP/3 second, and the official Solana RPC third. Successful PublicNode transaction queries wait 0.2 seconds; successful official-RPC transaction queries wait 3 seconds. Each run saves the chosen route, interval and connection history. The updated decoder reads creator from the creation instruction arguments. The stored September dataset retains its original records.
