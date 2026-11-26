"""Small synthetic regressions; no subscriptions are fetched or nodes contacted."""
import argparse
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "subscribe"))

import yaml
import artifacts
from outbound.base import proxy_exists
from outbound.common import QuotedStr, connection_key
from supplemental import load_public_sources


def node(**changes):
    result = {"name": "synthetic", "type": "vless", "server": "example.invalid", "port": 443,
              "uuid": "00000000-0000-4000-8000-000000000001", "tls": True,
              "network": "ws", "ws-opts": {"path": "/CaseSensitive"},
              "reality-opts": {"short-id": QuotedStr("00112233")}}
    result.update(changes)
    return result


class ArtifactTests(unittest.TestCase):
    def test_display_metadata_does_not_change_identity(self):
        self.assertEqual(connection_key(node()), connection_key(node(name="renamed", sub="SECRET", liveness=False)))

    def test_transport_and_reality_variants_are_distinct(self):
        first = node()
        variants = [node(network="grpc"), node(tls=False),
                    node(**{"ws-opts": {"path": "/Different"}}),
                    node(**{"reality-opts": {"short-id": "aabbccdd"}})]
        hosts = {"example.invalid:443": [first]}
        for variant in variants:
            self.assertFalse(proxy_exists(variant, hosts))
        self.assertTrue(proxy_exists(node(name="other"), hosts))

    def test_http_auth_is_not_deduplicated_by_endpoint_alone(self):
        first = node(type="http", username="First", password="MixedCase")
        second = node(type="http", username="Second", password="MixedCase")
        self.assertFalse(proxy_exists(second, {"example.invalid:443": [first]}))

    def test_export_preserves_credentials_and_quotes(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = "SyntheticCaseSensitivePassword"
            first = node(password=secret, sub="SECRET-SUBSCRIPTION-TOKEN", liveness=False)
            original = copy.deepcopy(first)
            report = artifacts.write_collection(directory, [[first, node(name="duplicate")]],
                                                 [("source-0001", [first]), ("source-0002", [node()])])
            data = yaml.safe_load(Path(directory, "aggregate.yaml").read_text())
            self.assertEqual(data["proxies"][0]["password"], secret)
            self.assertEqual(data["proxies"][0]["reality-opts"]["short-id"], "00112233")
            self.assertEqual(data["proxies"][0]["ws-opts"]["path"], "/CaseSensitive")
            self.assertNotIn("sub", data["proxies"][0])
            self.assertNotIn("liveness", data["proxies"][0])
            self.assertNotIn(secret, json.dumps(report))
            self.assertNotIn("SECRET-SUBSCRIPTION-TOKEN", json.dumps(report))
            self.assertNotIn("example.invalid", json.dumps(report))
            self.assertEqual(first, original)

    def test_collection_dedup_matches_validator_for_checker_provenance(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
        from validate_checked_output import validate_collection

        first = node(sub_url="https://example.invalid/?token=synthetic-first", sub_tag="source-a")
        duplicate = node(name="duplicate", sub_url="https://example.invalid/?token=synthetic-second",
                         sub_tag="source-b")
        different = node(**{"ws-opts": {"path": "/Different"}}, sub_tag="source-c")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = artifacts.write_collection(directory, [[first], [duplicate, different]],
                                                [("source-0001", [first]), ("source-0002", [duplicate, different])])
            parsed, checked = validate_collection(root / "aggregate.yaml", root / "collection-report.json")
            self.assertEqual(report["input_count"], 3)
            self.assertEqual(checked["unique_count"], 2)
            self.assertEqual({entry["ws-opts"]["path"] for entry in parsed}, {"/CaseSensitive", "/Different"})
            self.assertEqual(checked["node_sources"][artifacts.node_fingerprint(first)], ["source-0001", "source-0002"])
            for artifact in root.glob("*.yaml"):
                for proxy in yaml.safe_load(artifact.read_text())["proxies"]:
                    self.assertNotIn("sub_url", proxy)
                    self.assertNotIn("sub_tag", proxy)

    def test_merge_is_global_and_names_are_unique(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = node(), node(network="grpc")
            report = artifacts.write_collection(directory, [[first], [first, second]],
                                                 [("source-0001", [first]), ("source-0002", [first, second])])
            data = yaml.safe_load(Path(directory, "aggregate.yaml").read_text())
            self.assertEqual(report["unique_count"], 2)
            self.assertEqual(report["input_count"], 3)
            self.assertEqual(len({n["name"] for n in data["proxies"]}), 2)
            self.assertEqual(report["sources"][1]["unique_contribution"], 1)

    def test_duplicate_site_preserves_both_output_routes(self):
        import pipeline
        from config.models import GroupConfig, NodeInput, SiteConfig
        sites = [SiteConfig(name="first", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["a"]),
                 SiteConfig(name="second", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["b"])]
        tasks, grouped = pipeline.assign_sites(sites, {"a": GroupConfig(name="a"), "b": GroupConfig(name="b")},
                                              retry=1, bin_name="unused", special_protocols=True)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(grouped["a"], grouped["b"])

    def test_repeated_identical_filters_do_not_mutate_shared_task(self):
        import pipeline
        from config.models import GroupConfig, NodeInput, SiteConfig
        routes = ["a", "b", "c", "d"]
        sites = [SiteConfig(name=f"source-{route}", nodes=NodeInput(subscribe="https://example.invalid/sub"),
                            push_to=[route], include="US|HK", exclude="Expired") for route in routes]
        tasks, grouped = pipeline.assign_sites(sites, {route: GroupConfig(name=route) for route in routes},
                                              retry=1, bin_name="unused", special_protocols=True)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].include, "US|HK")
        self.assertEqual(tasks[0].exclude, "Expired")
        self.assertTrue(all(grouped[route] == [tasks[0].taskid] for route in routes))

    def test_distinct_inline_node_inputs_are_not_collapsed_by_empty_domain(self):
        import pipeline
        from config.models import GroupConfig, NodeInput, SiteConfig
        sites = [SiteConfig(name="first", nodes=NodeInput(proxies=[node()]), push_to=["all"]),
                 SiteConfig(name="second", nodes=NodeInput(proxies=[node(network="grpc")]), push_to=["all"])]
        tasks, grouped = pipeline.assign_sites(sites, {"all": GroupConfig(name="all")},
                                              retry=1, bin_name="unused", special_protocols=True)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(len(grouped["all"]), 2)

    def test_same_subscription_with_stricter_policy_gets_its_own_task(self):
        import pipeline
        from config.models import GroupConfig, NodeInput, SiteConfig
        sites = [SiteConfig(name="loose", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["a"],
                            check_alive=False, require_tls=False),
                 SiteConfig(name="strict", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["b"],
                            check_alive=True, require_tls=True)]
        tasks, grouped = pipeline.assign_sites(sites, {"a": GroupConfig(name="a"), "b": GroupConfig(name="b")},
                                              retry=1, bin_name="unused", special_protocols=True)
        self.assertEqual(len(tasks), 2)
        strict = next(task for task in tasks if task.taskid == grouped["b"][0])
        self.assertTrue(strict.check_alive)
        self.assertTrue(strict.require_tls)

    def test_same_source_multiple_policies_validates_across_components(self):
        import pipeline
        from config.models import GroupConfig, NodeInput, SiteConfig
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
        from validate_checked_output import validate_collection
        sites = [SiteConfig(name="loose", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["a"], require_tls=False),
                 SiteConfig(name="strict", nodes=NodeInput(subscribe="https://example.invalid/sub"), push_to=["b"], require_tls=True)]
        tasks, _ = pipeline.assign_sites(sites, {"a": GroupConfig(name="a"), "b": GroupConfig(name="b")},
                                         retry=1, bin_name="unused", special_protocols=True)
        self.assertEqual(len(tasks), 2)
        ids = [artifacts.source_fingerprint(task) for task in tasks]
        self.assertEqual(ids[0], ids[1])
        nodes = [node(tls=False), node(tls=True)]
        with tempfile.TemporaryDirectory() as directory:
            report = artifacts.write_collection(directory, [[nodes[0]], [nodes[1]]],
                                                 [(ids[0], [nodes[0]]), (ids[1], [nodes[1]])])
            parsed, verified = validate_collection(Path(directory, "aggregate.yaml"), Path(directory, "collection-report.json"))
            self.assertEqual(len(parsed), 2)
            self.assertEqual(len(verified["sources"]), 1)
            self.assertEqual(report["sources"][0]["parsed"], 2)
            self.assertTrue(all(entries == [ids[0]] for entries in report["node_sources"].values()))

    def test_previous_generation_is_invalidated_on_empty_and_early_failure(self):
        import process
        with tempfile.TemporaryDirectory() as directory:
            artifacts.write_collection(directory, [[node()], [node(network="grpc")]], [("source-1", [node()])])
            artifacts.write_collection(directory, [[node()]], [("source-1", [node()])])
            self.assertFalse(Path(directory, "group-0002-mihomo.yaml").exists())
            with self.assertRaises(ValueError):
                artifacts.write_collection(directory, [], [])
            self.assertFalse(Path(directory, "aggregate.yaml").exists())
            self.assertFalse(Path(directory, "collection-report.json").exists())
            artifacts.write_collection(directory, [[node()]], [("source-1", [node()])])
            args = argparse.Namespace(output_dir=directory, skip_push=True, invisible=True, retry=1,
                                      server="synthetic", check=False, num=1, mode=0, sources_file="")
            with patch.object(process, "load_configs", side_effect=ValueError("synthetic failure")), \
                 patch.object(process.executable, "which_bin", return_value=("unused", "unused")):
                with self.assertRaises(ValueError):
                    process.aggregate(args)
            self.assertFalse(Path(directory, "collection-report.json").exists())
            self.assertFalse(Path(directory, "aggregate.yaml").exists())

    def test_yaml_errors_do_not_log_original_passwords(self):
        import airport
        content = "proxies:\n - name: fake\n   password: SyntheticSecretNeverPublish: broken"
        with patch.object(airport.logger, "error") as error:
            self.assertEqual(airport.AirPort.decode(content, program="unused", do_verify=False), [])
        self.assertNotIn("SyntheticSecretNeverPublish", repr(error.call_args_list))

    def test_dependent_node_is_rejected_without_deleting_route(self):
        from outbound.base import verify
        dependent = node(**{"dialer-proxy": "RequiredDependency"})
        self.assertFalse(verify(dependent))
        self.assertEqual(dependent["dialer-proxy"], "RequiredDependency")

    def test_mieru_transport_is_canonical_before_export(self):
        # Parse-layer canonicalization covers every protocol, including ones
        # outside the validator's supported scope (mieru since the scope was
        # narrowed to vless + anytls).
        from outbound.base import verify
        for value, expected in ((None, "TCP"), ("tcp", "TCP"), ("udp", "UDP")):
            candidate = {"name": "synthetic-mieru", "type": "mieru", "server": "fixture.invalid", "port": 443,
                         "username": "CaseSensitiveUser", "password": "CaseSensitivePassword"}
            if value is not None:
                candidate["transport"] = value
            self.assertTrue(verify(candidate))
            self.assertEqual(candidate["transport"], expected)
            self.assertEqual(candidate["username"], "CaseSensitiveUser")
            self.assertEqual(candidate["password"], "CaseSensitivePassword")

    def test_native_yaml_bypasses_lossy_converter(self):
        from airport import AirPort
        import airport
        original = node(**{"reality-opts": {"public-key": "SyntheticKey", "short-id": "00112233"}})
        content = yaml.safe_dump({"proxies": [original]})
        with patch.object(airport.subconverter, "convert", side_effect=AssertionError("converter must not run")):
            parsed = AirPort.decode(content, program="must-not-run", do_verify=False)
        self.assertEqual(parsed, [original])

    def test_versioned_public_registry_is_small_and_valid(self):
        path = Path(__file__).resolve().parents[1] / "subscribe/examples/public-sources.json"
        sites = load_public_sources(str(path), ["all"])
        self.assertGreater(len(sites), 0)
        self.assertLessEqual(len(sites), 3)

    def test_empty_results_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                artifacts.write_collection(directory, [], [])

    def test_public_registry_routes_and_rejects_secret_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "sources.json")
            record = {"id": "synthetic", "url": "https://raw.githubusercontent.com/owner/repo/main/nodes.txt"}
            path.write_text(json.dumps({"schema_version": 1, "sources": [record]}))
            self.assertEqual(load_public_sources(str(path), ["all"])[0].push_to, ["all"])
            for bad in (record["url"] + "?token=secret", "http://raw.githubusercontent.com/a/b/main/nodes.txt",
                        "https://localhost/nodes.txt"):
                record["url"] = bad
                path.write_text(json.dumps({"schema_version": 1, "sources": [record]}))
                with self.assertRaises(ValueError):
                    load_public_sources(str(path), ["all"])

    def test_remote_write_policy_blocks_plugins_and_restores_environment(self):
        import os
        import push
        from config.models import StorageItem
        previous = os.environ.get("AGGREGATOR_SKIP_REMOTE_PUSH")
        store = push.PushToGist("synthetic-token")
        with patch.object(push.urllib.request, "urlopen", side_effect=AssertionError("network forbidden")):
            with push.remote_write_policy(enabled=False):
                self.assertEqual(os.environ.get("AGGREGATOR_SKIP_REMOTE_PUSH"), "1")
                self.assertFalse(store.push_to("synthetic-nodes", StorageItem()))
        self.assertEqual(os.environ.get("AGGREGATOR_SKIP_REMOTE_PUSH"), previous)

    def test_local_aggregate_skips_conversion_and_all_remote_publishing(self):
        import process
        from config.models import GroupConfig, NodeInput, ProcessConfig
        from workflow import TaskConfig
        config = ProcessConfig(groups={"all": GroupConfig(name="all")})
        task = TaskConfig(name="synthetic", bin_name="unused", taskid=1, nodes=NodeInput(uris=["vless://synthetic"]))
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(server="", check=False, num=1, invisible=True, retry=1, mode=0,
                                      overwrite=True, flexible=False, special_protocols=True, skip_alive_check=True,
                                      skip_remark=True, timeout=1, url="https://example.invalid/204",
                                      output_dir=directory, skip_push=True, sources_file="")
            with patch.object(process, "load_configs", return_value=config), \
                 patch.object(process, "assign", return_value=([task], {"all": [1]})), \
                 patch.object(process.executable, "which_bin", return_value=("unused", "unused")), \
                 patch.object(process.utils, "multi_process_run", return_value=[(1, [node(sub="secret")])]), \
                 patch.object(process.pipeline, "check_alive_proxies", side_effect=lambda **kw: kw["proxies"]), \
                 patch.object(process.push, "get_instance") as push, \
                 patch.object(process.subconverter, "convert") as convert, \
                 patch.object(process.workflow, "refresh") as refresh:
                process.aggregate(args)
                self.assertTrue(Path(directory, "aggregate.yaml").is_file())
                push.return_value.push_to.assert_not_called()
                convert.assert_not_called()
                refresh.assert_not_called()
                args.skip_push, args.output_dir = False, ""
                process.utils.multi_process_run.return_value = [(1, [])]
                with self.assertRaises(ValueError):
                    process.aggregate(args)
                refresh.assert_not_called()
                args.output_dir = str(Path(directory, "failed-publish"))
                config.groups["all"].targets = {"clash": "synthetic-store"}
                process.utils.multi_process_run.return_value = [(1, [node()])]
                convert.return_value = False
                Path(directory, "subconverter").mkdir()
                with patch.object(process, "PATH", directory), \
                     patch.object(process.subconverter, "generate_conf", return_value=True):
                    with self.assertRaises(ValueError):
                        process.aggregate(args)
                refresh.assert_not_called()
                self.assertFalse(Path(args.output_dir, "collection-report.json").exists())


if __name__ == "__main__":
    unittest.main()
