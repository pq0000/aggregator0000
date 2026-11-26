#!/usr/bin/env python3
"""Fail-closed, credential-safe evidence and publication for subs-check v1.6.5.

Upstream contract (tag 3c320fd): results.json lives beside each config in
cache/<config stem>, contains checkedAt/speedTest/mediaCheck/nodes, and records
only final passing nodes. all.yaml has no alive field. SUCCESS_COUNT is sent to
the callback after saving, even if a save failed. /api/status exposes actual
stage counters when the private, authenticated web API is enabled.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import io
import ipaddress
import json
import logging
import os
import re
import secrets
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

def _marker_cli(argv: list[str]) -> int:
    """Invalidate only the two fixed success markers, even without YAML deps."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("initialize-markers", "cleanup-markers"))
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--reason", default="check_not_completed")
    parser.add_argument("--phase", default="prepare")
    args = parser.parse_args(argv)
    expected = {"commit_sha": os.environ.get("GITHUB_SHA", "local"), "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"), "repository": os.environ.get("GITHUB_REPOSITORY", "local")}
    paths = (args.report.absolute(), args.evidence.absolute())
    try:
        if paths[0].name != "validation-report.json" or paths[1].name != "evidence.json" or paths[0].parent != paths[1].parent:
            raise ValueError
        # Reject links before making any directory, reading or overwriting a marker.
        if any(path.is_symlink() or any(parent.is_symlink() for parent in path.parents) for path in paths):
            raise ValueError
        reason = args.reason if re.fullmatch(r"[a-z0-9_]+", args.reason) else "check_not_completed"
        phase = args.phase if args.phase in ("prepare", "primary", "recheck", "finalize") else "unknown"
        preserve = None
        if args.command == "cleanup-markers" and paths[0].is_file():
            try:
                previous = json.loads(paths[0].read_text(encoding="utf-8"))
                if (isinstance(previous, dict) and previous.get("status") == "failed" and previous.get("complete") is False
                        and all(str(previous.get(key)) == str(value) for key, value in expected.items())
                        and re.fullmatch(r"[a-z0-9_]+", str(previous.get("failure_code", "")))
                        and previous["failure_code"] != "check_not_completed"):
                    preserve = previous
                    reason = previous["failure_code"]
                    phase = previous.get("phase", phase)
            except (OSError, ValueError, TypeError):
                pass
        payload = {"schema_version": 1, **expected, "timestamp": datetime.now(timezone.utc).isoformat(),
                   "status": "incomplete" if args.command == "initialize-markers" else "failed", "complete": False,
                   "failure_code": reason, "phase": phase}
        for index, path in enumerate(paths):
            if index == 0 and preserve is not None:
                continue  # Keep this invocation's precise validation failure.
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pipeline-marker-", delete=False) as stream:
                os.chmod(stream.name, 0o600)
                stream.write((json.dumps(payload, sort_keys=True) + "\n").encode())
                temporary = Path(stream.name)
            # Both resolved caller-owned fixed destinations were checked above;
            # this never deletes the output directory or promotes old all.yaml.
            os.replace(temporary, path)
    except (OSError, ValueError):
        print("[pipeline] status=failed code=unsafe_or_unwritable_success_marker", file=sys.stderr)
        return 1
    return 0


# These two early commands deliberately do not import optional dependencies.
if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] in ("initialize-markers", "cleanup-markers"):
    raise SystemExit(_marker_cli(sys.argv[1:]))

import yaml

# Reuse the collector's complete-connection fingerprint, not another identity.
# Prevent its legacy logger from creating a file merely because this tool imports it.
if not logging.getLogger().handlers:
    logging.getLogger().addHandler(logging.NullHandler())
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "subscribe"))
from artifacts import export_node, node_fingerprint  # noqa: E402

SUPPORTED_PROTOCOLS = frozenset({"vless", "anytls"})
SUBS_CHECK_VERSION = "1.6.5"
SUBS_CHECK_COMMIT = "3c320fd58aff5235e16218c050ec5b8ce587e233"
RECHECK_URL = "https://cp.cloudflare.com/generate_204"
MAX_AGE = 4 * 60 * 60
COUNTERS = ("total", "aliveDone", "alivePass", "mediaDone", "filterPass", "speedDone", "speedPass")
ARTIFACT_FILES = ("raw/aggregate.yaml", "raw/collection-report.json", "checked/filtered.yaml", "checked/filter-manifest.json")
ARTIFACT_KEY_CONTEXT = b"aggregator-round-artifact-key-v1\0"
ARTIFACT_AAD_CONTEXT = b"aggregator-round-artifact-aad-v1\0"


class ValidationError(ValueError):
    """A fixed error code; never include a node, YAML snippet, URI, or exception text."""


class StrictLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str) or key in result:
            raise ValidationError("yaml_duplicate_or_nonstring_key")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(value) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError):
        raise ValidationError("invalid_timestamp") from None


def fresh(value, *, started_at=None, max_age=MAX_AGE) -> None:
    stamp = parse_time(value)
    now = datetime.now(timezone.utc)
    if (now - stamp).total_seconds() > max_age or (stamp - now).total_seconds() > 5:
        raise ValidationError("stale_or_future_evidence")
    if started_at is not None and stamp < parse_time(started_at):
        raise ValidationError("evidence_predates_round")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_bytes(path: Path, content: bytes) -> None:
    path = Path(path).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValidationError("symlink_destination")
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pipeline-", delete=False) as stream:
        os.chmod(stream.name, 0o600)
        stream.write(content)
        temporary = Path(stream.name)
    # The checked absolute destination is the explicit caller-supplied file, not a glob.
    os.replace(temporary, path)


def write_json(path: Path, value: dict) -> None:
    atomic_bytes(path, (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode())


def load_json(path: Path) -> dict:
    path = Path(path).absolute()
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
        raise ValidationError("symlink_input")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        raise ValidationError("missing_or_invalid_json") from None
    if not isinstance(value, dict):
        raise ValidationError("json_object_required")
    return value


def load_yaml(path: Path) -> dict:
    try:
        value = yaml.load(Path(path).read_text(encoding="utf-8"), Loader=StrictLoader)
        # Reject YAML dates, sets, binary scalars, NaN and cyclic aliases.
        json.dumps(value, allow_nan=False)
    except ValidationError:
        raise
    except (OSError, yaml.YAMLError, ValueError, TypeError, RecursionError, UnicodeError):
        raise ValidationError("missing_or_invalid_yaml") from None
    if not isinstance(value, dict):
        raise ValidationError("yaml_object_required")
    return value


def clean_checker_node(node: dict) -> dict:
    return export_node(node)


def fingerprint(node: dict) -> str:
    try:
        return node_fingerprint(clean_checker_node(node))
    except (TypeError, ValueError, RecursionError):
        raise ValidationError("invalid_connection_structure") from None


def _text(node, key) -> str:
    value = node.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("incomplete_credentials_or_endpoint")
    return value


def _port(value) -> bool:
    return not isinstance(value, bool) and (isinstance(value, int) or isinstance(value, str) and value.isdecimal()) and 1 <= int(value) <= 65535


def _ranges(value, *, single=False) -> bool:
    if not isinstance(value, str) or not value:
        return False
    parts = [value] if single else re.split(r"[,/]", value)
    for part in parts:
        if not re.fullmatch(r"\d+(?:-\d+)?", part):
            return False
        bounds = part.split("-")
        if not all(_port(bound) for bound in bounds) or int(bounds[0]) > int(bounds[-1]):
            return False
    return True


def validate_node(node: dict) -> None:
    if not isinstance(node, dict):
        raise ValidationError("proxy_object_required")
    protocol = _text(node, "type")
    if protocol not in SUPPORTED_PROTOCOLS:
        raise ValidationError("unsupported_protocol")
    _text(node, "name")
    server = _text(node, "server")
    if any(char.isspace() for char in server) or any(char in server for char in "/@?#"):
        raise ValidationError("invalid_endpoint")
    for key in ("tls", "udp", "skip-cert-verify", "tfo", "mptcp"):
        if key in node and not isinstance(node[key], bool):
            raise ValidationError("invalid_boolean_option")
    if node.get("skip-cert-verify") is True:
        raise ValidationError("insecure_skip_cert_verify")
    if protocol == "anytls":
        if not _port(node.get("port")):
            raise ValidationError("invalid_port")
        _text(node, "password")
    else:
        if not _port(node.get("port")):
            raise ValidationError("invalid_port")
        if protocol == "vless":
            try:
                uuid.UUID(_text(node, "uuid"))
            except (ValueError, AttributeError):
                raise ValidationError("invalid_uuid") from None
            if node.get("tls") is not True and "reality-opts" not in node:
                raise ValidationError("vless_requires_tls_or_reality")
            if node.get("network", "") not in ("", "tcp", "ws", "grpc", "h2", "http", "xhttp"):
                raise ValidationError("invalid_vless_transport")
            if node.get("flow") and (node["flow"] not in ("xtls-rprx-vision", "xtls-rprx-vision-udp443") or node.get("tls") is not True):
                raise ValidationError("invalid_vless_flow")
            if "reality-opts" in node:
                opts = node["reality-opts"]
                if not isinstance(opts, dict) or node.get("tls") is not True:
                    raise ValidationError("invalid_reality_options")
                try:
                    key = _text(opts, "public-key")
                    decoded = base64.b64decode(key + "=" * (-len(key) % 4), altchars=b"-_", validate=True)
                except (ValueError, TypeError):
                    raise ValidationError("invalid_reality_key") from None
                if len(decoded) != 32 or not re.fullmatch(r"(?:[0-9a-fA-F]{2}){0,8}", opts.get("short-id", "")):
                    raise ValidationError("invalid_reality_key_or_short_id")

    for key in ("ws-opts", "grpc-opts", "h2-opts", "http-opts", "xhttp-opts", "smux"):
        if key in node and not isinstance(node[key], dict):
            raise ValidationError("invalid_transport_options")
    fingerprint(node)


def proxies(path: Path, *, nonempty=True, credentials=True) -> list[dict]:
    document = load_yaml(path)
    nodes = document.get("proxies")
    if not isinstance(nodes, list) or (nonempty and not nodes) or any(not isinstance(node, dict) for node in nodes):
        raise ValidationError("nonempty_proxies_required")
    if credentials:
        for node in nodes:
            validate_node(node)
        if len({fingerprint(node) for node in nodes}) != len(nodes):
            raise ValidationError("duplicate_connection")
        if len({node["name"] for node in nodes}) != len(nodes):
            raise ValidationError("duplicate_proxy_name")
    return nodes


def identity() -> dict:
    return {"commit_sha": os.environ.get("GITHUB_SHA", "local"), "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"), "repository": os.environ.get("GITHUB_REPOSITORY", "local")}


def match_identity(value: dict, expected=None) -> None:
    expected = identity() if expected is None else expected
    for key in ("commit_sha", "run_id", "run_attempt"):
        if str(value.get(key, "")) != str(expected[key]):
            raise ValidationError("wrong_run_or_commit")
    if "repository" in value and value["repository"] != expected.get("repository", identity()["repository"]):
        raise ValidationError("wrong_repository")


def validate_collection(aggregate: Path, report_path: Path, expected=None) -> tuple[list[dict], dict]:
    report = load_json(report_path)
    match_identity(report, expected)
    fresh(report.get("collected_at"))
    nodes = proxies(aggregate, credentials=False)
    ids = {fingerprint(node) for node in nodes}
    if report.get("schema_version") != 1 or report.get("unique_count") != len(nodes) or len(ids) != len(nodes):
        raise ValidationError("collection_count_mismatch")
    if report.get("aggregate_sha256") != sha256_file(aggregate):
        raise ValidationError("collection_hash_mismatch")
    sources = report.get("sources")
    mapping = report.get("node_sources")
    if not isinstance(sources, list) or not isinstance(mapping, dict):
        raise ValidationError("missing_contribution_evidence")
    source_ids = set()
    for source in sources:
        if not isinstance(source, dict) or not re.fullmatch(r"source-[a-zA-Z0-9_-]{4,64}", str(source.get("source_id", ""))):
            raise ValidationError("invalid_redacted_source_id")
        if source["source_id"] in source_ids:
            raise ValidationError("duplicate_source_id")
        source_ids.add(source["source_id"])
        for key in ("parsed", "unique", "in_aggregate", "unique_contribution", "overlap"):
            if not isinstance(source.get(key), int) or isinstance(source[key], bool) or source[key] < 0:
                raise ValidationError("invalid_contribution_count")
    if set(mapping) != ids:
        raise ValidationError("unattributed_or_unknown_connection")
    for entries in mapping.values():
        if not isinstance(entries, list) or not entries or len(entries) != len(set(entries)) or any(entry not in source_ids for entry in entries):
            raise ValidationError("invalid_contribution_mapping")
    return nodes, report


def upstream_dedup_key(node: dict) -> str:
    # Match v1.6.5 proxy/dedup.go's fmt.Sprintf colon concatenation, including
    # its delimiter ambiguities. A tuple would miss collisions containing ':'.
    # Type, username and key fields are absent in that upstream key.
    text = lambda key: node.get(key) if isinstance(node.get(key), str) else ""
    port = node.get("port")
    port_text = "<nil>" if port is None else str(port)
    return ":".join((text("server"), port_text, text("servername"),
                     text("password") or text("uuid"), text("sni"), text("network")))


def _artifact_key(meta: dict) -> bytes:
    token = os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise ValidationError("missing_artifact_encryption_secret")
    context = ARTIFACT_KEY_CONTEXT + json.dumps(meta, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(token.encode(), context, hashlib.sha256).digest()


def pack_artifact(root: Path, destination: Path) -> None:
    from Cryptodome.Cipher import AES
    meta = {"schema_version": 1, **identity()}
    header = json.dumps(meta, sort_keys=True, separators=(",", ":")).encode()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for name in ARTIFACT_FILES:
            bundle.writestr(name, (root / name).read_bytes())
    cipher = AES.new(_artifact_key(meta), AES.MODE_GCM, nonce=secrets.token_bytes(12))
    cipher.update(ARTIFACT_AAD_CONTEXT + header)
    encrypted, tag = cipher.encrypt_and_digest(archive.getvalue())
    atomic_bytes(destination, b"AGGREGATOR-ROUND-V1\n" + str(len(header)).encode() + b"\n" + header + cipher.nonce + tag + encrypted)
    print(f"[artifact] timestamp={utc_now()} encrypted=true files={len(ARTIFACT_FILES)}")


def unpack_artifact(source: Path, root: Path) -> None:
    from Cryptodome.Cipher import AES
    try:
        magic, size, remaining = source.read_bytes().split(b"\n", 2)
        length = int(size)
        if magic != b"AGGREGATOR-ROUND-V1" or not 0 < length < 4096:
            raise ValueError
        header, payload = remaining[:length], remaining[length:]
        meta = json.loads(header)
        match_identity(meta)
        if meta.get("repository") != identity()["repository"] or meta.get("schema_version") != 1:
            raise ValidationError("wrong_artifact_repository")
        cipher = AES.new(_artifact_key(meta), AES.MODE_GCM, nonce=payload[:12])
        cipher.update(ARTIFACT_AAD_CONTEXT + header)
        plain = cipher.decrypt_and_verify(payload[28:], payload[12:28])
        with zipfile.ZipFile(io.BytesIO(plain)) as bundle:
            if sorted(bundle.namelist()) != sorted(ARTIFACT_FILES) or sum(info.file_size for info in bundle.infolist()) > 256 * 1024 * 1024:
                raise ValueError
            files = {name: bundle.read(name) for name in ARTIFACT_FILES}
        for name, content in files.items():
            atomic_bytes(root / name, content)
        validate_collection(root / "raw/aggregate.yaml", root / "raw/collection-report.json")
    except ValidationError:
        raise
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile):
        raise ValidationError("artifact_authentication_or_structure_failed") from None
    print(f"[artifact] timestamp={utc_now()} decrypted=true current_run=true")


def prepare_session(args) -> None:
    if os.environ.get("SUB_CHECK_SKIP"):
        raise ValidationError("check_bypass_forbidden")
    _, collection = validate_collection(args.aggregate, args.collection_report)
    nodes = proxies(args.input)
    filtered = load_json(args.filter_manifest)
    match_identity(filtered)
    if filtered.get("candidate_sha256") != sha256_file(args.input) or filtered.get("aggregate_sha256") != collection["aggregate_sha256"]:
        raise ValidationError("filtered_artifact_mismatch")
    if filtered.get("kept_count") != len(nodes) or not {fingerprint(node) for node in nodes} <= set(collection["node_sources"]):
        raise ValidationError("unknown_filtered_connection")
    if args.run_dir.exists():
        raise ValidationError("run_directory_must_be_new")
    args.run_dir.mkdir(parents=True, mode=0o700)
    base = load_yaml(args.config)
    if not str(base.get("alive-test-url", "")).startswith("https://") or not str(base.get("speed-test-url", "")).startswith("https://"):
        raise ValidationError("https_checks_required")
    if not isinstance(base.get("min-speed"), int) or base["min-speed"] < 300:
        raise ValidationError("strict_speed_threshold_required")
    for key, ceiling in (("concurrent", 32), ("speed-concurrent", 4), ("media-concurrent", 4)):
        if not isinstance(base.get(key), int) or not 1 <= base[key] <= ceiling:
            raise ValidationError("unsafe_concurrency")
    atomic_bytes(args.run_dir / "input.yaml", args.input.read_bytes())
    # Safe report references only; no node fields or runtime API key in the session file.
    write_json(args.run_dir / "session.json", {"schema_version": 1, **identity(), "nonce": secrets.token_hex(24),
               "created_at": utc_now(), "collection_path": str(args.collection_report.resolve()),
               "aggregate_path": str(args.aggregate.resolve()), "filter_manifest_path": str(args.filter_manifest.resolve()),
               "input_sha256": sha256_file(args.input), "serve_url": args.serve_url, "api_port": args.api_port})
    atomic_bytes(args.run_dir / "base-config.yaml", yaml.safe_dump(base, allow_unicode=True, sort_keys=False).encode())


def prepare_phase(run_dir: Path, phase: str) -> None:
    session = load_json(run_dir / "session.json")
    match_identity(session)
    base = load_yaml(run_dir / "base-config.yaml")
    phase_dir = run_dir / phase
    phase_dir.mkdir(mode=0o700)
    if phase == "primary":
        source = run_dir / "input.yaml"
    else:
        validate_phase(run_dir, "primary")
        source = run_dir / "primary/output/all.yaml"
        base.update({"alive-test-url": RECHECK_URL, "speed-test-url": "", "media-check": False,
                     "filter": [], "filter-regex": "", "platforms": []})
    atomic_bytes(phase_dir / "input.yaml", source.read_bytes())
    base.update({"sub-urls": [session["serve_url"]], "sub-urls-remote": [], "sub-urls-concurrent": 1,
                 "save-method": "local", "output-dir": str((phase_dir / "output").resolve()),
                 "keep-days": 0, "success-limit": 0, "cron-expression": "", "check-interval": 1440,
                 "rename-node": False, "node-type": sorted(SUPPORTED_PROTOCOLS), "sub-store-port": "",
                 "mihomo-api-url": "", "mihomo-api-secret": "", "proxy": "", "apprise-api-server": "",
                 "recipient-url": [], "enable-web-ui": True, "api-key": secrets.token_hex(24),
                 "listen-port": f"127.0.0.1:{session['api_port']}", "print-progress": False,
                 "callback-script": str((Path(__file__).resolve().parents[1] / "subscribe/subs-check-conf/callback.sh"))})
    for key in ("github-token", "github-gist-id", "worker-token", "webdav-password", "s3-secret-key"):
        base[key] = ""
    atomic_bytes(phase_dir / "config.yaml", yaml.safe_dump(base, allow_unicode=True, sort_keys=False).encode())
    write_json(phase_dir / "round.json", {"schema_version": 1, **identity(), "nonce": session["nonce"], "phase": phase,
               "started_at": utc_now(), "input_sha256": sha256_file(phase_dir / "input.yaml"),
               "input_count": len(proxies(phase_dir / "input.yaml")), "alive_test_url": base["alive-test-url"],
               "speed_test": bool(base["speed-test-url"]), "media_check": bool(base["media-check"]),
               "min_speed": base["min-speed"], "config_sha256": sha256_file(phase_dir / "config.yaml")})


def _api(phase_dir: Path, route: str) -> dict:
    config = load_yaml(phase_dir / "config.yaml")
    request = urllib.request.Request(f"http://{config['listen-port']}/api/{route}", headers={"X-API-Key": config["api-key"]})
    # Never route evidence traffic through an externally configured proxy.
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=10) as response:
        value = json.loads(response.read(1024 * 1024))
    if not isinstance(value, dict):
        raise ValidationError("invalid_status_response")
    return value


def record_callback(phase_dir: Path, parent_pid: int) -> None:
    meta = load_json(phase_dir / "round.json")
    match_identity(meta)
    result = {"schema_version": 1, **identity(), "nonce": meta["nonce"], "phase": meta["phase"],
              "completed_at": utc_now(), "parent_pid": parent_pid, "status": "callback", "success_count": -1}
    try:
        result["success_count"] = int(os.environ.get("SUCCESS_COUNT", "-1"))
        status = _api(phase_dir, "status")
        pipeline = status.get("pipeline")
        if not isinstance(pipeline, dict) or any(not isinstance(pipeline.get(key), int) or isinstance(pipeline[key], bool) for key in COUNTERS):
            raise ValidationError("missing_stage_counters")
        result["pipeline"] = {key: pipeline[key] for key in COUNTERS}
        result["phase_finished"] = status.get("phase") == 0
        result["has_speed_test"] = status.get("hasSpeedTest")
        result["version"] = _api(phase_dir, "version").get("version")
        result["snapshot_sha256"] = sha256_file(phase_dir / "cache/config/results.json")
        result["output_sha256"] = sha256_file(phase_dir / "output/all.yaml")
    except ValidationError as error:
        result.update({"status": "failed", "failure_code": str(error)})
    except Exception:
        result.update({"status": "failed", "failure_code": "callback_evidence_unavailable"})
    # Even an empty/failed round signals completion, so only our supervisor stops
    # the checker promptly. This marker alone NEVER grants publication.
    write_json(phase_dir / "callback.json", result)


def record_exit(phase_dir: Path, reason: str, exit_code: int) -> None:
    write_json(phase_dir / "termination.json", {"reason": reason, "exit_code": exit_code, "finished_at": utc_now()})


def _counter_checks(counters: dict, total: int, kept: int, speed_test: bool) -> None:
    if any(not isinstance(counters.get(key), int) or isinstance(counters[key], bool) or counters[key] < 0 for key in COUNTERS):
        raise ValidationError("invalid_stage_counters")
    if counters["total"] != total or counters["aliveDone"] != total:
        raise ValidationError("incomplete_alive_stage")
    if not 0 <= counters["filterPass"] <= counters["mediaDone"] == counters["alivePass"] <= total:
        raise ValidationError("incomplete_media_stage")
    if speed_test:
        if counters["speedDone"] != counters["filterPass"] or not 0 <= counters["speedPass"] == kept <= counters["speedDone"]:
            raise ValidationError("incomplete_speed_stage")
    elif kept != counters["filterPass"] or counters["speedDone"] != 0 or counters["speedPass"] != 0:
        raise ValidationError("unexpected_recheck_speed_results")


def validate_phase(run_dir: Path, phase: str) -> tuple[list[dict], dict]:
    session = load_json(run_dir / "session.json")
    match_identity(session)
    directory = run_dir / phase
    meta = load_json(directory / "round.json")
    callback = load_json(directory / "callback.json")
    termination = load_json(directory / "termination.json")
    for value in (meta, callback):
        match_identity(value)
        if value.get("nonce") != session["nonce"] or value.get("phase") != phase:
            raise ValidationError("wrong_round_nonce_or_phase")
    if termination.get("reason") != "callback" or termination.get("exit_code") not in (0, 143):
        raise ValidationError("timeout_or_failed_checker")
    if callback.get("status") != "callback" or callback.get("phase_finished") is not True:
        raise ValidationError("callback_not_completed")
    try:
        pid = int((directory / "checker.pid").read_text(encoding="ascii"))
    except (OSError, ValueError):
        raise ValidationError("missing_child_pid") from None
    if callback.get("parent_pid") != pid or pid <= 1:
        raise ValidationError("callback_from_wrong_process")
    version = str(callback.get("version", ""))
    if not re.fullmatch(r"v?1\.6\.5-3c320fd[0-9a-f]{0,33}", version):
        raise ValidationError("wrong_checker_version")
    fresh(meta["started_at"])
    fresh(callback["completed_at"], started_at=meta["started_at"])
    if sha256_file(directory / "config.yaml") != meta["config_sha256"]:
        raise ValidationError("runtime_config_changed")
    source = proxies(directory / "input.yaml")
    if sha256_file(directory / "input.yaml") != meta["input_sha256"] or len(source) != meta["input_count"]:
        raise ValidationError("input_changed_during_check")
    nodes = proxies(directory / "output/all.yaml")
    snapshot_path = directory / "cache/config/results.json"
    snapshot = load_json(snapshot_path)
    fresh(snapshot.get("checkedAt"), started_at=meta["started_at"])
    if parse_time(snapshot["checkedAt"]) > parse_time(callback["completed_at"]):
        raise ValidationError("snapshot_after_callback")
    if snapshot.get("speedTest") is not meta["speed_test"] or snapshot.get("mediaCheck") is not meta["media_check"] or callback.get("has_speed_test") is not meta["speed_test"]:
        raise ValidationError("wrong_snapshot_test_mode")
    if callback.get("snapshot_sha256") != sha256_file(snapshot_path) or callback.get("output_sha256") != sha256_file(directory / "output/all.yaml"):
        raise ValidationError("result_changed_after_callback")
    for path in (snapshot_path, directory / "output/all.yaml"):
        if path.stat().st_mtime < parse_time(meta["started_at"]).timestamp():
            raise ValidationError("stale_output_file")
    records = snapshot.get("nodes")
    if not isinstance(records, list) or len(records) != len(nodes) or callback.get("success_count") != len(nodes):
        raise ValidationError("snapshot_callback_count_mismatch")
    # Snapshot records have endpoint/display fields, not credentials. Match them
    # one-for-one to the YAML, then match COMPLETE YAML identities to this input.
    remaining = Counter((node["name"], node["type"], node["server"], str(node.get("port", ""))) for node in nodes)
    for record in records:
        if not isinstance(record, dict):
            raise ValidationError("invalid_snapshot_record")
        key = (record.get("name"), record.get("type"), record.get("server"), str(record.get("port", "")))
        if remaining[key] <= 0:
            raise ValidationError("snapshot_output_mismatch")
        remaining[key] -= 1
        speed = record.get("speed")
        if not isinstance(speed, int) or isinstance(speed, bool) or (meta["speed_test"] and speed < meta["min_speed"]) or (not meta["speed_test"] and speed != 0):
            raise ValidationError("speed_threshold_or_mode_mismatch")
    if any(remaining.values()) or not {fingerprint(node) for node in nodes} <= {fingerprint(node) for node in source}:
        raise ValidationError("untested_or_mutated_connection")
    counters = callback["pipeline"]
    _counter_checks(counters, len(source), len(nodes), meta["speed_test"])
    if phase == "recheck" and (meta["speed_test"] or meta["media_check"] or meta["alive_test_url"] != RECHECK_URL):
        raise ValidationError("recheck_mode_not_strict")
    summary = {"phase": phase, "complete": True, "checked_at": snapshot["checkedAt"], "version": version,
               "input_count": len(source), "passed": len(nodes), "speed_test": meta["speed_test"],
               "media_check": meta["media_check"], "min_speed_kbps": meta["min_speed"], "pipeline": counters,
               "alive_test_url": meta["alive_test_url"], "output_sha256": callback["output_sha256"],
               "snapshot_sha256": callback["snapshot_sha256"], "callback_sha256": sha256_file(directory / "callback.json")}
    write_json(directory / "verified.json", summary)
    return nodes, summary


def finalize(run_dir: Path, output: Path, evidence: Path, report_path: Path) -> dict:
    session = load_json(run_dir / "session.json")
    original, primary = validate_phase(run_dir, "primary")
    checked, recheck = validate_phase(run_dir, "recheck")
    if sha256_file(run_dir / "recheck/input.yaml") != primary["output_sha256"]:
        raise ValidationError("recheck_not_of_primary_output")
    if primary["alive_test_url"] == recheck["alive_test_url"] or not primary["alive_test_url"].startswith("https://"):
        raise ValidationError("distinct_https_targets_required")
    _, collection = validate_collection(Path(session["aggregate_path"]), Path(session["collection_path"]))
    filtered = load_json(Path(session["filter_manifest_path"]))
    ids = {fingerprint(node) for node in checked}
    publish_nodes = [clean_checker_node(node) for node in original if fingerprint(node) in ids]
    if not publish_nodes or len(publish_nodes) != len(checked):
        raise ValidationError("empty_or_unknown_publish_output")
    counts = {"collected": collection["unique_count"], "eligible": filtered["kept_count"],
              "isolated": filtered.get("quarantined_count", 0), "not_target": filtered.get("not_target_count", 0),
              "primary_alive_failed": primary["input_count"] - primary["pipeline"]["alivePass"],
              "media_or_policy_filtered": primary["pipeline"]["alivePass"] - primary["pipeline"]["filterPass"],
              # v1.6.5 drops both below-threshold and speed request failures;
              # it does NOT retain enough evidence to split those two counts.
              "speed_failed_or_slow": primary["pipeline"]["filterPass"] - primary["passed"],
              "https_recheck_failed": recheck["input_count"] - recheck["passed"], "published": len(publish_nodes)}
    contribution = []
    assigned = set()
    for source in collection["sources"]:
        accepted = {fp for fp in ids if source["source_id"] in collection["node_sources"].get(fp, [])}
        unique = accepted - assigned
        assigned.update(accepted)
        contribution.append({"source_id": source["source_id"], "parsed": source["parsed"],
                             "in_aggregate": source["in_aggregate"], "published": len(accepted), "unique_contribution": len(unique)})
    payload = yaml.safe_dump({"proxies": publish_nodes}, allow_unicode=True, sort_keys=False).encode()
    # Atomic promotion happens ONLY after both actual rounds and all validation.
    atomic_bytes(output, payload)
    result = {"schema_version": 1, **identity(), "status": "passed", "complete": True, "timestamp": utc_now(),
              "session_dir": str(run_dir.resolve()), "aggregate_sha256": collection["aggregate_sha256"],
              "collection_sha256": sha256_file(Path(session["collection_path"])), "output_sha256": sha256_file(output),
              "counts": counts, "protocols": dict(Counter(node["type"] for node in publish_nodes)),
              "isolation_reasons": filtered.get("isolation_reasons", {}), "sources": contribution,
              "primary": primary, "recheck": recheck}
    write_json(evidence, result)
    # Deliberately omit private paths, per-node hashes, API keys and raw snapshots.
    report = {key: value for key, value in result.items() if key not in {"session_dir", "aggregate_sha256", "collection_sha256", "output_sha256", "primary", "recheck"}}
    report["checked_at"] = recheck["checked_at"]
    report["checker_version"] = recheck["version"]
    report["speed_rejection_semantics"] = "failed_or_below_threshold; not classified as dead"
    write_json(report_path, report)
    print(f"[validate] timestamp={report['timestamp']} status=passed counts={json.dumps(counts, sort_keys=True)}")
    return result


def validate_final(output: Path, evidence: Path) -> tuple[str, dict]:
    proof = load_json(evidence)
    match_identity(proof)
    fresh(proof.get("timestamp"))
    if proof.get("status") != "passed" or proof.get("complete") is not True or proof.get("schema_version") != 1:
        raise ValidationError("publication_evidence_not_passed")
    if proof.get("output_sha256") != sha256_file(output):
        raise ValidationError("publication_output_hash_mismatch")
    nodes = proxies(output)
    if proof.get("counts", {}).get("published") != len(nodes):
        raise ValidationError("publication_count_mismatch")
    directory = Path(str(proof.get("session_dir", "")))
    session = load_json(directory / "session.json")
    match_identity(session)
    _, collection = validate_collection(Path(session["aggregate_path"]), Path(session["collection_path"]))
    if collection["aggregate_sha256"] != proof.get("aggregate_sha256") or sha256_file(Path(session["collection_path"])) != proof.get("collection_sha256"):
        raise ValidationError("publication_collection_mismatch")
    first, primary = validate_phase(directory, "primary")
    second, recheck = validate_phase(directory, "recheck")
    if proof.get("primary") != primary or proof.get("recheck") != recheck or sha256_file(directory / "recheck/input.yaml") != primary["output_sha256"]:
        raise ValidationError("publication_round_proof_mismatch")
    if {fingerprint(node) for node in nodes} != {fingerprint(node) for node in second} or not {fingerprint(node) for node in second} <= {fingerprint(node) for node in first}:
        raise ValidationError("publication_contains_untested_connection")
    if not primary["speed_test"] or recheck["speed_test"] or primary["alive_test_url"] == recheck["alive_test_url"]:
        raise ValidationError("publication_missing_strict_dual_target_proof")
    return output.read_text(encoding="utf-8"), proof


def publish(output: Path, evidence: Path, gist_id: str) -> None:
    content, proof = validate_final(output, evidence)
    token = os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise ValidationError("missing_publish_secret")
    if not re.fullmatch(r"[0-9a-fA-F]{20,64}", gist_id):
        raise ValidationError("invalid_gist_id")
    request = urllib.request.Request(f"https://api.github.com/gists/{gist_id}",
              data=json.dumps({"files": {"all.yaml": {"content": content}}}).encode(), method="PATCH",
              headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/vnd.github+json",
                       "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "aggregator-validated-publisher"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 200:
                raise ValidationError("gist_publish_non_success")
    except urllib.error.HTTPError as error:
        raise ValidationError(f"gist_publish_http_{error.code}") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ValidationError("gist_publish_transport_failed") from None
    print(f"[publish] timestamp={utc_now()} status=200 nodes={proof['counts']['published']}")


def failed_report(path: Path, reason: str, phase="unknown") -> None:
    write_json(path, {"schema_version": 1, **identity(), "timestamp": utc_now(), "status": "failed", "complete": False,
                     "failure_code": reason if re.fullmatch(r"[a-z0-9_]+", reason) else "internal_error", "phase": phase})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("verify-collection")
    command.add_argument("--aggregate", type=Path, required=True)
    command.add_argument("--collection-report", type=Path, required=True)
    for action in ("pack-artifact", "unpack-artifact"):
        command = commands.add_parser(action)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument("--file", type=Path, required=True)
    command = commands.add_parser("prepare-session")
    for option in ("input", "aggregate", "collection-report", "filter-manifest", "config", "run-dir"):
        command.add_argument("--" + option, type=Path, required=True)
    command.add_argument("--serve-url", required=True)
    command.add_argument("--api-port", type=int, required=True)
    command = commands.add_parser("prepare-phase")
    command.add_argument("--run-dir", type=Path, required=True)
    command.add_argument("--phase", choices=("primary", "recheck"), required=True)
    command = commands.add_parser("record-callback")
    command.add_argument("--phase-dir", type=Path, required=True)
    command.add_argument("--parent-pid", type=int, required=True)
    command = commands.add_parser("record-exit")
    command.add_argument("--phase-dir", type=Path, required=True)
    command.add_argument("--reason", choices=("callback", "timeout", "error"), required=True)
    command.add_argument("--exit-code", type=int, required=True)
    command = commands.add_parser("validate-phase")
    command.add_argument("--run-dir", type=Path, required=True)
    command.add_argument("--phase", choices=("primary", "recheck"), required=True)
    command.add_argument("--report", type=Path)
    command = commands.add_parser("finalize")
    command.add_argument("--run-dir", type=Path, required=True)
    command.add_argument("--output", type=Path, required=True)
    command.add_argument("--evidence", type=Path, required=True)
    command.add_argument("--report", type=Path, required=True)
    for action in ("validate", "publish"):
        command = commands.add_parser(action)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--evidence", type=Path, required=True)
        command.add_argument("--report", type=Path)
        if action == "publish":
            command.add_argument("--gist-id", default=os.environ.get("GIST_ID", ""))
    command = commands.add_parser("failure")
    command.add_argument("--report", type=Path, required=True)
    command.add_argument("--reason", required=True)
    command.add_argument("--phase", default="unknown")
    args = parser.parse_args()
    try:
        if args.command == "verify-collection":
            nodes, report = validate_collection(args.aggregate, args.collection_report)
            print(f"[collection] timestamp={utc_now()} nodes={len(nodes)} current_run=true")
            input_count = report.get("input_count")
            if type(input_count) is int and input_count >= len(nodes):
                print(f"[collection_counts] parsed={input_count} unique={len(nodes)}")
        elif args.command == "pack-artifact":
            pack_artifact(args.root, args.file)
        elif args.command == "unpack-artifact":
            unpack_artifact(args.file, args.root)
        elif args.command == "prepare-session":
            prepare_session(args)
        elif args.command == "prepare-phase":
            prepare_phase(args.run_dir, args.phase)
        elif args.command == "record-callback":
            record_callback(args.phase_dir, args.parent_pid)
        elif args.command == "record-exit":
            record_exit(args.phase_dir, args.reason, args.exit_code)
        elif args.command == "validate-phase":
            _, report = validate_phase(args.run_dir, args.phase)
            print(f"[check] timestamp={utc_now()} phase={args.phase} input={report['input_count']} passed={report['passed']} complete=true")
        elif args.command == "finalize":
            finalize(args.run_dir, args.output, args.evidence, args.report)
        elif args.command == "validate":
            _, proof = validate_final(args.output, args.evidence)
            print(f"[validate] timestamp={utc_now()} nodes={proof['counts']['published']} complete=true")
        elif args.command == "publish":
            publish(args.output, args.evidence, args.gist_id)
        elif args.command == "failure":
            failed_report(args.report, args.reason, args.phase)
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else "pipeline_internal_error"
        if getattr(args, "report", None):
            failed_report(args.report, reason, getattr(args, "phase", "unknown"))
        print(f"[pipeline] timestamp={utc_now()} status=failed code={reason}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
