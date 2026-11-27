#!/usr/bin/env python3
"""Select supported, structurally valid candidates without pretending isolation is a test."""
from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools"))
from validate_checked_output import (  # noqa: E402
    SUPPORTED_PROTOCOLS, ValidationError, atomic_bytes, fingerprint, identity,
    proxies, sha256_file, upstream_dedup_key, utc_now, validate_collection,
    validate_node, write_json,
)
import yaml  # noqa: E402

TARGET_PROTOCOLS = SUPPORTED_PROTOCOLS


def _strip_empty(value):
    if isinstance(value, dict):
        return {key: _strip_empty(item) for key, item in value.items() if item not in ("", None)}
    if isinstance(value, list):
        return [_strip_empty(item) for item in value]
    return value


def equivalence_key(node: dict) -> str:
    """Functional identity of a listing inside one engine-dedup bucket.

    Two listings that agree under this key are the same connection republished
    with a different representation (port as text, empty optional fields,
    tls:false spelled out) or a different uTLS client-fingerprint only. The
    fingerprint is a client-side disguise preference: whichever representative
    is kept is the exact config the engine tests and the publisher releases,
    so collapsing them cannot publish an untested connection. Any other
    difference (transport options, REALITY material, ALPN, credentials) keeps
    the listings distinct and the whole bucket stays isolated.
    """
    config = copy.deepcopy(node)
    config.pop("name", None)
    config.pop("client-fingerprint", None)
    if isinstance(config.get("port"), str) and config["port"].isdecimal():
        config["port"] = int(config["port"])
    for key in ("type", "server"):
        if isinstance(config.get(key), str):
            config[key] = config[key].strip().lower()
    if not config.get("tls"):
        config.pop("tls", None)
    return json.dumps(_strip_empty(config), sort_keys=True, ensure_ascii=False)


def filter_file(source: Path, destination: Path, *, collection_report: Path | None = None,
                manifest: Path | None = None) -> int:
    if collection_report is not None:
        nodes, report = validate_collection(source, collection_report)
    else:
        nodes, report = proxies(source, credentials=False), None
    candidates = []
    isolated = []
    seen = set()
    not_target = duplicates = 0
    for index, raw in enumerate(nodes, 1):
        node = copy.deepcopy(raw)
        if node.get("type") not in TARGET_PROTOCOLS:
            not_target += 1
            continue
        # Neither source names nor provenance tags are used in check logs or API
        # records. Connection fields (including credentials/options) stay intact.
        node["name"] = f"candidate-{index:06d}"
        try:
            validate_node(node)
        except ValidationError as error:
            isolated.append({"candidate_index": index, "protocol": node["type"], "reason": str(error)})
            continue
        fp = fingerprint(node)
        if fp in seen:
            duplicates += 1
            continue
        seen.add(fp)
        candidates.append((index, node))
    buckets = defaultdict(list)
    for index, node in candidates:
        buckets[upstream_dedup_key(node)].append((index, node))
    kept = []
    equivalent_duplicates = 0
    for bucket in buckets.values():
        if len(bucket) == 1:
            kept.extend(bucket)
        elif len({equivalence_key(node) for _, node in bucket}) == 1:
            # One functional connection listed several times (representation
            # or client-fingerprint variants). Keep the first listing as the
            # sole representative: it alone is tested and may be published.
            bucket.sort(key=lambda item: item[0])
            kept.append(bucket[0])
            equivalent_duplicates += len(bucket) - 1
        else:
            # The pinned engine would silently collapse these distinct full
            # connections. Isolate ALL members, not an arbitrary "alive" winner.
            isolated.extend({"candidate_index": index, "protocol": node["type"], "reason": "engine_dedup_collision"}
                            for index, node in bucket)
    kept.sort(key=lambda item: item[0])
    if not kept:
        raise ValidationError("no_testable_target_nodes")
    output = [node for _, node in kept]
    atomic_bytes(destination, yaml.safe_dump({"proxies": output}, allow_unicode=True, sort_keys=False).encode())
    result = {"schema_version": 1, **identity(), "timestamp": utc_now(), "input_count": len(nodes),
              "kept_count": len(output), "not_target_count": not_target, "duplicate_count": duplicates,
              "equivalent_duplicate_count": equivalent_duplicates,
              "quarantined_count": len(isolated), "protocols": dict(Counter(node["type"] for node in output)),
              "isolation_reasons": dict(Counter(item["reason"] for item in isolated)),
              "isolation_protocols": dict(Counter(item["protocol"] for item in isolated)),
              "isolated_candidates": isolated, "aggregate_sha256": sha256_file(source),
              "candidate_sha256": sha256_file(destination), "engine": "subs-check-1.6.5/mihomo-1.19.31"}
    if report is not None:
        result["collection_sha256"] = sha256_file(collection_report)
    if manifest is not None:
        write_json(manifest, result)
    print(f"[filter] timestamp={result['timestamp']} input={len(nodes)} eligible={len(output)} "
          f"not_target={not_target} isolated={len(isolated)} duplicates={duplicates} "
          f"equivalent_duplicates={equivalent_duplicates} "
          f"protocols={json.dumps(result['protocols'], sort_keys=True)}")
    return len(output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--collection-report", type=Path)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    try:
        filter_file(args.source, args.destination, collection_report=args.collection_report, manifest=args.manifest)
    except Exception as error:
        code = str(error) if isinstance(error, ValidationError) else "filter_internal_error"
        print(f"[filter] timestamp={utc_now()} status=failed code={code}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
