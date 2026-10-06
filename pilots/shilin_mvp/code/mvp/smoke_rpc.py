"""Small live check for the DKUCC common CPU path.

This asks one IPv4 Solana RPC for a few recent Pump.fun signatures, decodes at
most a handful of transactions, and fetches one declared metadata URI. It does
not start the 24-hour cohort or the 36-hour follow-up wait.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .acquire_chain import FallbackRpcClient, PUMPFUN_PROGRAM_ID, decode_transaction
from .common import read_json, read_csv, write_json
from .observe_metadata import HttpTransport


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", default=str(Path(__file__).resolve().parents[1] / "configs" / "mvp_protocol.json"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--signature-limit", type=int, default=20)
    parser.add_argument("--transaction-limit", type=int, default=5)
    args = parser.parse_args()
    protocol = read_json(args.protocol)
    output = Path(args.output)
    source_path = Path(args.protocol).with_name("source_register.csv")
    sources = read_csv(source_path) if source_path.exists() else None
    program_id = str(protocol.get("pumpfun_program_id", PUMPFUN_PROGRAM_ID))
    client = FallbackRpcClient(protocol, output_dir=output, source_register=sources)
    try:
        health = client.call("getHealth", [])
    except Exception as exc:
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        write_json(output / "smoke_result.json", {"endpoint": client.endpoint, "address_family": "ipv4", "error": f"{exc.__class__.__name__}: {exc}", "rpc_calls": client.calls})
        print(f"smoke failed after retries: {exc}")
        print(f"wrote {output / 'smoke_result.json'}")
        client.close()
        return
    try:
        signatures = client.call("getSignaturesForAddress", [program_id, {"limit": args.signature_limit}])
    except Exception as exc:
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        write_json(output / "smoke_result.json", {"endpoint": client.endpoint, "address_family": "ipv4", "health": health.get("result"), "error": f"{exc.__class__.__name__}: {exc}", "rpc_calls": client.calls})
        print(f"smoke failed after retries: {exc}")
        print(f"wrote {output / 'smoke_result.json'}")
        client.close()
        return
    page = signatures.get("result") or []
    events = []
    fetched = 0
    errors = []
    for item in page:
        if fetched >= args.transaction_limit or events:
            break
        if item.get("err") is not None or not item.get("signature"):
            continue
        signature = str(item["signature"])
        try:
            transaction = client.call("getTransaction", [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}])
        except RuntimeError as exc:
            errors.append({"signature": signature, "error": str(exc)[:300]})
            continue
        fetched += 1
        events.extend(decode_transaction(transaction, signature, item.get("blockTime"), program_id))
    metadata = None
    uri = next((event.get("metadata_uri", "") for event in events if event.get("metadata_uri")), "")
    if uri:
        result = HttpTransport(timeout=15)(uri)
        metadata = {"uri": uri, "status": result.status, "http_status": result.http_status, "bytes": len(result.body), "error_class": result.error_class}
    report = {
        "endpoint": client.endpoint,
        "address_family": "ipv4",
        "health": health.get("result"),
        "signatures_returned": len(page),
        "transactions_fetched": fetched,
        "creates_decoded": len(events),
        "sample_event": events[0] if events else None,
        "metadata": metadata,
        "errors": errors,
        "rpc_calls": client.calls,
    }
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "smoke_result.json", report)
    print(f"endpoint {client.endpoint}")
    print(f"health {report['health']}")
    print(f"signatures {report['signatures_returned']} transactions {fetched} creates {len(events)}")
    if metadata:
        print(f"metadata {metadata['status']} {metadata['http_status']} bytes {metadata['bytes']}")
    print(f"wrote {output / 'smoke_result.json'}")
    client.close()


if __name__ == "__main__":
    main()
