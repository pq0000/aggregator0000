"""Offline tests for the previous-round seed cache; all Gist access is mocked."""
from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "subscribe"))

import yaml

import artifacts
import seedcache

SEED_NODE = {"name": "seed-node", "type": "vless", "server": "seed.invalid", "port": 443,
             "uuid": "22222222-2222-4222-8222-222222222222", "tls": True, "network": "tcp"}
OTHER_NODE = {"name": "fresh-node", "type": "vless", "server": "fresh.invalid", "port": 8443,
              "uuid": "33333333-3333-4333-8333-333333333333", "tls": True, "network": "tcp"}


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _gist_opener(payload: dict):
    def opener(request, timeout=0):
        return _Response(json.dumps(payload).encode("utf-8"))
    return opener


def _gist_payload(nodes: list) -> dict:
    content = yaml.safe_dump({"proxies": nodes}, allow_unicode=True)
    return {"files": {"all.yaml": {"content": content, "truncated": False}}}


class FetchSeedNodesTests(unittest.TestCase):
    def test_success_returns_parsed_nodes(self):
        nodes, status = seedcache.fetch_seed_nodes(
            gist_id="a" * 32, token="synthetic",
            opener=_gist_opener(_gist_payload([SEED_NODE, OTHER_NODE])))
        self.assertEqual(status, "ok")
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0]["server"], "seed.invalid")

    def test_network_failure_is_fail_open(self):
        def opener(request, timeout=0):
            raise urllib.error.URLError("synthetic outage")
        nodes, status = seedcache.fetch_seed_nodes(gist_id="a" * 32, token="synthetic", opener=opener)
        self.assertEqual(nodes, [])
        self.assertEqual(status, "fetch_failed")

    def test_empty_product_is_fail_open(self):
        nodes, status = seedcache.fetch_seed_nodes(
            gist_id="a" * 32, token="synthetic", opener=_gist_opener(_gist_payload([])))
        self.assertEqual(nodes, [])
        self.assertEqual(status, "seed_empty")

    def test_missing_seed_file_is_fail_open(self):
        nodes, status = seedcache.fetch_seed_nodes(
            gist_id="a" * 32, token="synthetic", opener=_gist_opener({"files": {}}))
        self.assertEqual(nodes, [])
        self.assertEqual(status, "seed_file_missing")

    def test_no_credentials_skips_fetch(self):
        def opener(request, timeout=0):  # must never be called
            raise AssertionError("fetch attempted without credentials")
        with patch.dict("os.environ", {}, clear=True):
            nodes, status = seedcache.fetch_seed_nodes(token=None, opener=opener)
        self.assertEqual(nodes, [])
        self.assertEqual(status, "no_credentials")


class SeedMergeTests(unittest.TestCase):
    def test_seed_source_is_attributed_in_collection_report(self):
        with tempfile.TemporaryDirectory() as directory:
            out = str(Path(directory) / "round")
            report = artifacts.write_collection(
                out,
                groups=[[OTHER_NODE], [SEED_NODE]],
                sources=[("source-fresh-fixture", [OTHER_NODE]),
                         (seedcache.SEED_SOURCE_ID, [SEED_NODE])],
                source_notes={seedcache.SEED_SOURCE_ID: "seed_cache:ok"},
            )
            by_id = {item["source_id"]: item for item in report["sources"]}
            seed = by_id[seedcache.SEED_SOURCE_ID]
            self.assertEqual(seed["parsed"], 1)
            self.assertEqual(seed["in_aggregate"], 1)
            self.assertEqual(seed["unique_contribution"], 1)
            self.assertEqual(seed["note"], "seed_cache:ok")
            aggregate = yaml.safe_load(Path(out, "aggregate.yaml").read_text())
            servers = {node["server"] for node in aggregate["proxies"]}
            self.assertEqual(servers, {"fresh.invalid", "seed.invalid"})

    def test_empty_seed_still_annotated(self):
        with tempfile.TemporaryDirectory() as directory:
            out = str(Path(directory) / "round")
            report = artifacts.write_collection(
                out,
                groups=[[OTHER_NODE]],
                sources=[("source-fresh-fixture", [OTHER_NODE]),
                         (seedcache.SEED_SOURCE_ID, [])],
                source_notes={seedcache.SEED_SOURCE_ID: "seed_cache:fetch_failed"},
            )
            by_id = {item["source_id"]: item for item in report["sources"]}
            seed = by_id[seedcache.SEED_SOURCE_ID]
            self.assertEqual(seed["parsed"], 0)
            self.assertEqual(seed["status"], "no_proxies")
            self.assertEqual(seed["note"], "seed_cache:fetch_failed")


if __name__ == "__main__":
    unittest.main()
