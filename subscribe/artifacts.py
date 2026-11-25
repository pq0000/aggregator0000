"""Credential-preserving local aggregation artifacts and redacted contribution data."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import yaml

from outbound.common import NODE_METADATA, connection_key, plain_value


def node_fingerprint(node: dict[str, object]) -> str:
    return hashlib.sha256(connection_key(node).encode("utf-8")).hexdigest()


def source_fingerprint(task: object) -> str:
    """Stable, non-reversible source labels across runs using the existing CI secret."""
    payload = {"subscribe": task.nodes.subscribe_list(), "uris": task.nodes.uris,
               "proxies": [connection_key(node) for node in task.nodes.proxies]}
    if task.nodes.empty():
        payload.update({"domain": task.domain, "index": task.index})
    key = (os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN") or "local-synthetic-only").encode()
    message = b"aggregator/source-id/v1\0" + json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    return "source-" + hmac.new(key, message, hashlib.sha256).hexdigest()[:20]


def export_node(node: dict[str, object]) -> dict[str, object]:
    # Keep the display name, but never include subscription tokens/provenance in YAML.
    return {str(key): plain_value(value) for key, value in node.items()
            if key == "name" or key not in NODE_METADATA}


def unique_nodes(nodes: list[dict[str, object]]) -> list[dict[str, object]]:
    result, seen, names = [], set(), set()
    for raw in nodes:
        node = export_node(raw)
        identity = node_fingerprint(node)
        if identity in seen:
            continue
        seen.add(identity)
        name = str(node.get("name") or "node")
        candidate, suffix = name, 1
        while candidate in names:
            suffix += 1
            candidate = f"{name}-{suffix}"
        node["name"] = candidate
        names.add(candidate)
        result.append(node)
    return result


def _clear_generation(root: Path) -> None:
    """Invalidate only our own prior outputs, including before an empty/failed run."""
    for candidate in root.iterdir():
        generated = candidate.name in {"aggregate.yaml", "collection-report.json"} or re.fullmatch(
            r"group-\d+-mihomo\.yaml", candidate.name)
        if generated:
            if candidate.parent.resolve() != root or candidate.is_dir():
                raise ValueError("unsafe generated artifact path")
            candidate.unlink()


def prepare_collection(directory: str) -> Path:
    root = Path(directory).resolve()
    current = Path.cwd().resolve()
    if root == Path(root.anchor) or root == current or root in current.parents:
        raise ValueError("output-dir must be a dedicated artifact directory")
    root.mkdir(parents=True, exist_ok=True)
    _clear_generation(root)
    return root


def _publish_generation(root: Path, files: dict[str, str]) -> None:
    # All documents are staged before any output is exposed; the report is the
    # commit marker and is always published last. Interrupted generations lack it.
    with tempfile.TemporaryDirectory(prefix=".collection-", dir=root) as directory:
        staging = Path(directory)
        for name, text in files.items():
            (staging / name).write_text(text, encoding="utf-8")
        for name in files:
            destination = root / name
            if destination.parent.resolve() != root:
                raise ValueError("unsafe artifact destination")
            (staging / name).replace(destination)


def write_collection(directory: str, groups: list[list[dict[str, object]]],
                     sources: list[tuple[str, list[dict[str, object]]]]) -> dict[str, object]:
    root = prepare_collection(directory)
    files: dict[str, str] = {}
    all_nodes = unique_nodes([node for group in groups for node in group])
    if not all_nodes:
        raise ValueError("refusing to export an empty aggregate")
    for index, group in enumerate(groups, 1):
        # Configuration group names must never become paths or appear in public reports.
        payload = {"proxies": unique_nodes(group)}
        files[f"group-{index:04d}-mihomo.yaml"] = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    text = yaml.safe_dump({"proxies": all_nodes}, allow_unicode=True, sort_keys=False)
    files["aggregate.yaml"] = text
    final_ids = {node_fingerprint(node) for node in all_nodes}
    node_sources: dict[str, list[str]] = {}
    metrics, contributed = [], set()
    # A physical subscription may legitimately have multiple processing policies.
    # Its stable source ID is shared, while contribution records/mappings are unique.
    grouped_sources: dict[str, list[dict[str, object]]] = {}
    for source_id, nodes in sources:
        grouped_sources.setdefault(source_id, []).extend(nodes)
    for source_id, nodes in grouped_sources.items():
        identities = {node_fingerprint(node) for node in nodes}
        accepted = identities & final_ids
        new = accepted - contributed
        metrics.append({"source_id": source_id, "parsed": len(nodes),
                        "unique": len(identities), "in_aggregate": len(accepted),
                        "unique_contribution": len(new), "overlap": len(accepted) - len(new),
                        "status": "parsed" if nodes else "no_proxies",
                        "protocols": dict(Counter(str(n.get("type", "unknown")) for n in nodes))})
        contributed.update(accepted)
        for identity in accepted:
            node_sources.setdefault(identity, []).append(source_id)
    report = {"schema_version": 1, "collected_at": datetime.now(timezone.utc).isoformat(),
              "commit_sha": os.environ.get("GITHUB_SHA", "local"),
              "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
              "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
              "input_count": sum(len(nodes) for _, nodes in sources),
              "unique_count": len(all_nodes),
              "aggregate_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
              "protocols": dict(Counter(str(n.get("type", "unknown")) for n in all_nodes)),
              "sources": metrics, "node_sources": node_sources}
    files["collection-report.json"] = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    _publish_generation(root, files)
    return report
