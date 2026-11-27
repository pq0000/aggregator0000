"""Small synthetic, offline tests; never execute a real checker or probe a node."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import validate_checked_output as validator

spec = importlib.util.spec_from_file_location("pipeline_filter", ROOT / ".github/workflows/scripts/filter_protocols.py")
filter_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(filter_module)
from artifacts import write_collection

NODE = {"name": "synthetic-private-name", "type": "vless", "server": "fixture.invalid", "port": 443,
        "uuid": "11111111-1111-4111-8111-111111111111", "tls": True, "network": "ws",
        "ws-opts": {"path": "/synthetic", "headers": {"Host": "fixture.invalid"}}}
MOCK_CHECKER = r'''#!/usr/bin/env python3
import copy, json, os, subprocess, sys, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import yaml
config = yaml.safe_load(Path(sys.argv[2]).read_text())
root = Path(sys.argv[2]).parent
nodes = yaml.safe_load((root / 'input.yaml').read_text())['proxies']
speed = bool(config['speed-test-url'])
mode = os.environ.get('MOCK_MODE', 'success')
output = Path(config['output-dir'])
output.mkdir(parents=True)
result_nodes = copy.deepcopy(nodes)
if mode == 'mutated_credential': result_nodes[0]['uuid'] = '22222222-2222-4222-8222-222222222222'
if mode == 'empty': result_nodes = []
if result_nodes: (output / 'all.yaml').write_text(yaml.safe_dump({'proxies': result_nodes}, sort_keys=False))
cache = root / 'cache/config'
cache.mkdir(parents=True)
records = [{'name': node['name'], 'type': node['type'], 'server': node['server'], 'port': str(node.get('port', '')),
            'speed': 500 if speed else 0} for node in result_nodes]
checked_at = '2001-01-01T00:00:00+00:00' if mode == 'stale' else datetime.now(timezone.utc).isoformat()
(cache / 'results.json').write_text(json.dumps({'checkedAt': checked_at, 'speedTest': speed,
                                             'mediaCheck': config['media-check'], 'nodes': records}))
if mode == 'exit_without_callback': sys.exit(0)
pipeline = {'total': len(nodes), 'aliveDone': len(nodes), 'alivePass': len(result_nodes),
            'mediaDone': len(result_nodes), 'filterPass': len(result_nodes),
            'speedDone': len(result_nodes) if speed else 0, 'speedPass': len(result_nodes) if speed else 0}
if mode == 'partial': pipeline['aliveDone'] -= 1
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('X-API-Key') != config['api-key']:
            self.send_error(401); return
        if self.path == '/api/status': value = {'phase': 0, 'hasSpeedTest': speed, 'pipeline': pipeline}
        elif self.path == '/api/version': value = {'version': '1.6.4-old' if mode == 'wrong_version' else '1.6.5-3c320fd'}
        else: self.send_error(404); return
        body = json.dumps(value).encode()
        self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *args): pass
host, port = config['listen-port'].rsplit(':', 1)
server = ThreadingHTTPServer((host, int(port)), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
print('SYNTHETIC-CREDENTIAL-DO-NOT-EXPOSE', flush=True)
if mode != 'timeout':
    env = dict(os.environ, SUCCESS_COUNT=str(len(result_nodes)))
    subprocess.run([config['callback-script']], env=env, check=True)
while True: time.sleep(1)
'''


def free_port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


class PipelineFixes(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pipeline-offline-")
        self.root = Path(self.temporary.name)
        self.env = patch.dict(os.environ, {"GITHUB_SHA": "a" * 40, "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "1",
                                          "GITHUB_REPOSITORY": "offline/fixture", "PUSH_TOKEN": "synthetic-encryption-test-token"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temporary.cleanup()

    def collection(self, nodes=None):
        nodes = [dict(NODE)] if nodes is None else nodes
        (self.root / "raw").mkdir(exist_ok=True)
        write_collection(str(self.root / "raw"), [nodes], [("source-0123456789abcdef0123", nodes)])
        with contextlib.redirect_stdout(io.StringIO()):
            filter_module.filter_file(self.root / "raw/aggregate.yaml", self.root / "checked/filtered.yaml",
                                      collection_report=self.root / "raw/collection-report.json",
                                      manifest=self.root / "checked/filter-manifest.json")

    def run_mock(self, mode="success", timeout="15", *, sentinel=False):
        self.collection()
        checker = self.root / "mock-subs-check"
        checker.write_text(MOCK_CHECKER)
        checker.chmod(0o755)
        serve_port, api_port = free_port(), free_port()
        while api_port == serve_port:
            api_port = free_port()
        environment = dict(os.environ, PYTHON_BIN=sys.executable, PYTHONDONTWRITEBYTECODE="1", MOCK_MODE=mode,
                           PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
                           SUBS_CHECK_BIN=str(checker), PIPELINE_OUTPUT_DIR=str(self.root),
                           SERVE_PORT=str(serve_port), SUBS_CHECK_API_PORT=str(api_port), SUBS_CHECK_TIMEOUT="90000")
        other = None
        if sentinel:
            other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(20)", "subs-check", "-f", "config.yaml"])
        try:
            started = time.monotonic()
            result = subprocess.run(["bash", str(ROOT / "run_subs_check.sh"), str(self.root / "checked/filtered.yaml"), timeout],
                                    cwd=ROOT, env=environment, capture_output=True, text=True, timeout=25)
            elapsed = time.monotonic() - started
            if other is not None:
                self.assertIsNone(other.poll(), "cleanup killed an unrelated process")
            self.assertNotIn("SYNTHETIC-CREDENTIAL-DO-NOT-EXPOSE", result.stdout + result.stderr)
            self.assertNotIn(NODE["uuid"], result.stdout + result.stderr)
            return result, elapsed
        finally:
            if other is not None:
                other.terminate()
                other.wait(timeout=3)

    def test_real_contract_without_yaml_alive_field(self):
        result, _ = self.run_mock()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        final = validator.proxies(self.root / "all.yaml")
        self.assertNotIn("alive", final[0])
        _, evidence = validator.validate_final(self.root / "all.yaml", self.root / "checked/evidence.json")
        self.assertEqual(evidence["counts"]["published"], 1)
        self.assertTrue(evidence["primary"]["speed_test"])
        self.assertFalse(evidence["recheck"]["speed_test"])
        self.assertNotEqual(evidence["primary"]["alive_test_url"], evidence["recheck"]["alive_test_url"])
        report_text = (self.root / "checked/validation-report.json").read_text()
        for sensitive in (NODE["uuid"], NODE["server"], NODE["name"], "api-key", "node_sources"):
            self.assertNotIn(sensitive, report_text)
        evidence["commit_sha"] = "b" * 40
        validator.write_json(self.root / "checked/evidence.json", evidence)
        with self.assertRaisesRegex(validator.ValidationError, "wrong_run_or_commit"):
            validator.validate_final(self.root / "all.yaml", self.root / "checked/evidence.json")

    def test_positional_timeout_rejects_old_output_and_preserves_other_process(self):
        old = "old, unchecked output must stay untouched\n"
        (self.root / "all.yaml").write_text(old)
        result, elapsed = self.run_mock("timeout", "1", sentinel=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertLess(elapsed, 8, "$2 was ignored in favor of the 90000 env timeout")
        self.assertEqual((self.root / "all.yaml").read_text(), old)
        self.assertEqual(validator.load_json(self.root / "checked/validation-report.json")["failure_code"], "checker_timeout")

    def test_early_invalid_timeout_invalidates_old_success_markers_without_checker(self):
        checked = self.root / "checked"
        checked.mkdir()
        old_report = {"schema_version": 1, **validator.identity(), "status": "passed", "complete": True}
        validator.write_json(checked / "validation-report.json", old_report)
        validator.write_json(checked / "evidence.json", old_report)
        old_yaml = "unchanged previous product\n"
        (self.root / "all.yaml").write_text(old_yaml)
        environment = dict(os.environ, PYTHON_BIN=sys.executable, PYTHONDONTWRITEBYTECODE="1", PIPELINE_OUTPUT_DIR=str(self.root))
        result = subprocess.run(["bash", str(ROOT / "run_subs_check.sh"), "missing-no-checker.yaml", "0"],
                                cwd=ROOT, env=environment, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        for name in ("validation-report.json", "evidence.json"):
            marker = validator.load_json(checked / name)
            self.assertEqual(marker["status"], "failed")
            self.assertFalse(marker["complete"])
            self.assertEqual(marker["failure_code"], "invalid_timeout")
        self.assertEqual((self.root / "all.yaml").read_text(), old_yaml)
        # No directory deletion and no following a marker symlink to another file.
        outside = self.root / "outside-marker.json"
        validator.write_json(outside, old_report)
        target = checked / "validation-report.json"
        self.assertFalse(target.is_symlink())
        self.assertEqual(target.resolve(), checked / "validation-report.json")
        target.unlink()
        target.symlink_to(outside)
        result = subprocess.run(["bash", str(ROOT / "run_subs_check.sh"), "missing-no-checker.yaml", "0"],
                                cwd=ROOT, env=environment, capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(validator.load_json(outside)["status"], "passed")
        with self.assertRaisesRegex(validator.ValidationError, "symlink_input"):
            validator.load_json(target)

    def test_file_and_exit_zero_without_callback_are_not_success(self):
        result, _ = self.run_mock("exit_without_callback")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "all.yaml").exists())

    def test_partial_counts_are_not_complete(self):
        result, _ = self.run_mock("partial")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(validator.load_json(self.root / "checked/validation-report.json")["failure_code"], "incomplete_alive_stage")

    def test_stale_snapshot_is_rejected(self):
        result, _ = self.run_mock("stale")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "all.yaml").exists())

    def test_empty_round_cannot_promote_or_reuse_old_yaml(self):
        result, _ = self.run_mock("empty")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "all.yaml").exists())

    def test_complete_connection_credentials_must_match_input(self):
        result, _ = self.run_mock("mutated_credential")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(validator.load_json(self.root / "checked/validation-report.json")["failure_code"], "untested_or_mutated_connection")

    def test_engine_version_is_actually_verified(self):
        result, _ = self.run_mock("wrong_version")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(validator.load_json(self.root / "checked/validation-report.json")["failure_code"], "wrong_checker_version")

    def test_engine_dedup_collisions_are_isolated_not_falsely_checked(self):
        collision = dict(NODE, name="same-key-different-transport", **{"ws-opts": {"path": "/different"}})
        unrelated = dict(NODE, name="testable", server="second-fixture.invalid")
        self.collection([NODE, collision, unrelated])
        manifest = validator.load_json(self.root / "checked/filter-manifest.json")
        self.assertEqual(manifest["kept_count"], 1)
        self.assertEqual(manifest["quarantined_count"], 2)
        self.assertEqual(manifest["isolation_reasons"]["engine_dedup_collision"], 2)
        # The pinned Go engine concatenates fields with ':' without escaping;
        # different tuples can therefore still collide in its actual string key.
        left = dict(NODE, servername="alpha:beta", password="gamma")
        right = dict(NODE, servername="alpha", password="beta:gamma")
        self.assertEqual(validator.upstream_dedup_key(left), validator.upstream_dedup_key(right))
        self.assertNotEqual(validator.fingerprint(left), validator.fingerprint(right))

    def test_protocol_scope_is_vless_and_anytls_only(self):
        config = yaml.safe_load((ROOT / "subscribe/subs-check-conf/config.yaml").read_text())
        self.assertEqual(set(config["node-type"]), set(filter_module.TARGET_PROTOCOLS))
        self.assertEqual(set(filter_module.TARGET_PROTOCOLS), {"vless", "anytls"})
        for protocol in ("masque", "mieru", "hysteria2", "ss", "trojan"):
            node = {"name": "offline-scope", "type": protocol, "server": "fixture.invalid", "port": 443}
            with self.assertRaisesRegex(validator.ValidationError, "unsupported_protocol"):
                validator.validate_node(node)

    def test_vless_requires_tls_or_reality_and_verified_certificates(self):
        plain = dict(NODE, tls=False)
        with self.assertRaisesRegex(validator.ValidationError, "vless_requires_tls_or_reality"):
            validator.validate_node(plain)
        skipping = dict(NODE, **{"skip-cert-verify": True})
        with self.assertRaisesRegex(validator.ValidationError, "insecure_skip_cert_verify"):
            validator.validate_node(skipping)
        validator.validate_node(dict(NODE))

    def test_invalid_credential_nodes_are_isolated(self):
        self.collection([dict(NODE, uuid=""), dict(NODE, server="valid-fixture.invalid")])
        manifest = validator.load_json(self.root / "checked/filter-manifest.json")
        self.assertEqual(manifest["quarantined_count"], 1)
        self.assertEqual(manifest["kept_count"], 1)

    def test_duplicate_yaml_keys_do_not_hide_credential_corruption(self):
        path = self.root / "ambiguous.yaml"
        path.write_text("proxies: []\nproxies: []\n")
        with self.assertRaisesRegex(validator.ValidationError, "yaml_duplicate"):
            validator.load_yaml(path)

    def test_encrypted_artifact_round_trip_and_run_binding(self):
        self.collection()
        sealed = self.root / "round.enc"
        with contextlib.redirect_stdout(io.StringIO()):
            validator.pack_artifact(self.root, sealed)
            validator.unpack_artifact(sealed, self.root / "decrypted")
        self.assertNotIn(NODE["uuid"].encode(), sealed.read_bytes())
        self.assertEqual((self.root / "decrypted/raw/aggregate.yaml").read_bytes(), (self.root / "raw/aggregate.yaml").read_bytes())
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "43"}):
            with self.assertRaisesRegex(validator.ValidationError, "wrong_run_or_commit"):
                validator.unpack_artifact(sealed, self.root / "wrong-run")
        self.assertFalse((self.root / "wrong-run/raw/aggregate.yaml").exists())
        body = bytearray(sealed.read_bytes())
        body[-1] ^= 1
        sealed.write_bytes(body)
        with self.assertRaisesRegex(validator.ValidationError, "artifact_authentication"):
            validator.unpack_artifact(sealed, self.root / "tampered")
        self.assertFalse((self.root / "tampered/raw/aggregate.yaml").exists())

    def test_authenticated_artifact_extra_paths_are_rejected_before_write(self):
        import zipfile
        from Cryptodome.Cipher import AES
        meta = {"schema_version": 1, **validator.identity()}
        header = json.dumps(meta, sort_keys=True, separators=(",", ":")).encode()
        for extra in ("../escape.yaml", "extra.yaml"):
            archive = io.BytesIO()
            with zipfile.ZipFile(archive, "w") as bundle:
                for name in validator.ARTIFACT_FILES:
                    bundle.writestr(name, b"synthetic")
                bundle.writestr(extra, b"never write")
            cipher = AES.new(validator._artifact_key(meta), AES.MODE_GCM, nonce=b"synthetic123")
            cipher.update(validator.ARTIFACT_AAD_CONTEXT + header)
            ciphertext, tag = cipher.encrypt_and_digest(archive.getvalue())
            sealed = self.root / "extra.enc"
            sealed.write_bytes(b"AGGREGATOR-ROUND-V1\n" + str(len(header)).encode() + b"\n" + header + cipher.nonce + tag + ciphertext)
            with self.assertRaises(validator.ValidationError):
                validator.unpack_artifact(sealed, self.root / "extra-output")
            self.assertFalse((self.root / "extra-output/raw/aggregate.yaml").exists())
            self.assertFalse((self.root / "escape.yaml").exists())

    def test_workflow_separates_pr_and_production_and_keeps_artifacts_encrypted(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/mihomo-pipeline.yaml").read_text())
        self.assertIn("pull_request", workflow["on"])
        self.assertIn("main", workflow["on"]["push"]["branches"])
        self.assertEqual(workflow["jobs"]["aggregate-and-filter"]["if"], "github.event_name != 'pull_request'")
        for job in workflow["jobs"].values():
            for step in job["steps"]:
                if step.get("uses", "").startswith("actions/checkout@"):
                    self.assertEqual(step["with"]["ref"], "${{ github.sha }}")
                if step.get("uses", "").startswith("actions/upload-artifact@"):
                    path = step["with"]["path"]
                    self.assertTrue(path.endswith(".enc") or path.endswith("validation-report.json"))
        self.assertNotIn("PUSH_TOKEN", workflow.get("env", {}))
        self.assertNotIn("gist.githubusercontent.com", (ROOT / ".github/workflows/mihomo-pipeline.yaml").read_text())


if __name__ == "__main__":
    unittest.main()
