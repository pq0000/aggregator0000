"""Small offline regressions using synthetic subscriptions and documentation IPv6 addresses."""

from __future__ import annotations

import base64
import json
import logging
import socket
import sys
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import Mock, patch

# Do not create workflow.log merely by importing the application logger.
if not logging.getLogger().handlers:
    logging.getLogger().addHandler(logging.NullHandler())
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "subscribe"))

with patch.object(socket.socket, "connect", side_effect=AssertionError("offline test")):
    from config.models import CrawlConfig, CrawlPersist, NodeInput, PageJob, StorageConfig, StorageItem, TaskParams
    from config.parse import Node
    from crawl import engine
    from crawl.channels.page import PageChannel
    from crawl.extract import extract_subscribes
    from crawl.models import ChannelResult, CrawlContext, NodeBatch, SubItem
    from push import PushTo


class OfflineSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "utils.http_get"):
            blocker = patch(target, side_effect=AssertionError("network forbidden in offline tests"))
            blocker.start()
            self.addCleanup(blocker.stop)

    @staticmethod
    def context(task: TaskParams | None = None) -> CrawlContext:
        return CrawlContext(
            mode=0, include_nodes=True, max_fails=2, exclude="", task=task or TaskParams(),
            storage=None, pushtool=None, num_threads=1, display=False,
        )

    def test_credentials_paths_and_scheme_case_remain_verbatim(self) -> None:
        uris = [
            "TrOjAn://FakePaSs%2F+~!$&*()=,:;@node.invalid:443/CaseSensitive/Path?x=AbC%2F#Label",
            "vless://00000000-0000-4000-8000-000000000001@node.invalid:443/?path=%2FUpper&security=tls#Name",
            "ss://aes-128-gcm:Fake'PaSs@node.invalid:8443/Case;Path~!",
        ]
        content = "\n".join(f'<a href="{uri}">synthetic</a>' for uri in uris)
        self.assertEqual(extract_subscribes(content, include_nodes=True).nodes.uris, uris)

    def test_ss_and_vmess_base64_payloads_are_not_lowercased(self) -> None:
        fake = {
            "v": "2", "ps": "SyntheticUpperCase", "add": "node.invalid", "port": "443",
            "id": "00000000-0000-4000-8000-000000000001", "path": "/CaseSensitive",
        }
        encoded_vmess = base64.b64encode(json.dumps(fake).encode()).decode()
        fake_ss = "aes-128-gcm:FakeCaseSensitivePass"
        encoded_ss = base64.b64encode(fake_ss.encode()).decode()
        legacy_ss = base64.b64encode((fake_ss + "@[2001:db8::1]:443").encode()).decode()
        uris = ["VMess://" + encoded_vmess, "ss://" + encoded_ss + "@[2001:db8::1]:443#Test", "ss://" + legacy_ss]
        extracted = extract_subscribes("\n".join(uris), include_nodes=True).nodes.uris
        self.assertEqual(extracted, uris)
        self.assertEqual(json.loads(base64.b64decode(extracted[0].split("://", 1)[1])), fake)
        self.assertEqual(base64.b64decode(extracted[1].split("://", 1)[1].split("@", 1)[0]).decode(), fake_ss)

    def test_ipv6_and_protocol_aliases(self) -> None:
        uris = [
            "hy2://FakeCase%25@[2001:db8::2]:443?insecure=0#Hy2",
            "Hysteria2://FakeCase@[2001:db8::3]:8443#H2",
            "mieru://FakeUser:FakePass@[2001:db8::4]:443?transport=tcp#Mieru",
            "masque://FakeUser:FakePass@[2001:db8::5]:443/CasePath#Masque",
        ]
        content = "\n".join(f"<a href='{uri}'>fake</a>" for uri in uris)
        self.assertEqual(extract_subscribes(content, include_nodes=True).nodes.uris, uris)

    def test_html_markdown_and_endpoint_punctuation_boundaries(self) -> None:
        uri = "trojan://FakeCase@[2001:db8::1]:443"
        samples = [f'<a href="{uri}">fake</a>', f"<a href='{uri}'>fake</a>", f"[fake]({uri})", uri + ".", uri + ","]
        for content in samples:
            with self.subTest(content=content):
                self.assertEqual(extract_subscribes(content, include_nodes=True).nodes.uris, [uri])
        self.assertEqual(extract_subscribes("notvmess://NotANode", include_nodes=True).nodes.uris, [])

    def test_converter_url_does_not_turn_base64_plus_into_space(self) -> None:
        uri = "ss://QWJj+RGVm/QUJD=="
        content = "https://convert.invalid/sub?target=clash&url=" + urllib.parse.quote(uri, safe="+")
        self.assertEqual(extract_subscribes(content, include_nodes=True).nodes.uris, [uri])

    def test_duplicate_routes_and_source_metadata_merge_with_priority(self) -> None:
        url = "https://sub.invalid/api/v1/client/subscribe?token=SyntheticTokenNeverLog"
        earlier_task = TaskParams(push_to=["alpha", "shared"], name="EarlierDisplay", rename="SameProcessing", max_rate=4.0, coupon="FakeEarlierCoupon")
        later_task = TaskParams(push_to=["beta", "shared"], name="PriorityDisplay", rename="SameProcessing", coupon="FakePriorityCoupon")
        earlier = SubItem(url, "PAGE", earlier_task, name="fallback", errors=2, discovered=True, debut=True, skip_cache=True)
        later = SubItem(url, "", later_task, ready=True, allow_nonstandard=True)
        with self.assertLogs(level="WARNING") as captured:
            collapsed = engine._collapse_items([earlier, later], defaults=TaskParams(max_rate=4.0))
        self.assertEqual(collapsed, [later])
        self.assertEqual(later.task.push_to, ["beta", "shared", "alpha"])
        self.assertEqual(later.task.rename, "SameProcessing")
        self.assertEqual(later.task.max_rate, 4.0)
        self.assertEqual((later.name, later.origin, later.errors), ("fallback", "PAGE", 2))
        self.assertTrue(later.skip_cache and later.discovered and later.debut and later.allow_nonstandard)
        self.assertEqual(earlier_task.push_to, ["alpha", "shared"])
        self.assertEqual(later_task.push_to, ["beta", "shared"])
        diagnostic = "\n".join(captured.output)
        self.assertIn("name", diagnostic)
        self.assertIn("coupon", diagnostic)
        for private in (url, "SyntheticTokenNeverLog", "FakeEarlierCoupon", "FakePriorityCoupon", "EarlierDisplay", "PriorityDisplay"):
            self.assertNotIn(private, diagnostic)

    def test_duplicate_inheritance_and_persist_only_priority(self) -> None:
        url = "https://sub.invalid/synthetic"
        inherited = SubItem(url, "PAGE", TaskParams(), persist_only=True)
        routed = SubItem(url, "TELEGRAM", TaskParams(push_to=["local"]))
        winner = engine._collapse_items([inherited, routed], TaskParams(push_to=["global"]))[0]
        self.assertIs(winner, routed)
        self.assertEqual(winner.task.push_to, ["local", "global"])
        self.assertFalse(winner.persist_only)
        empty = SubItem(url, "PAGE", TaskParams(push_to=[]))
        self.assertEqual(engine._collapse_items([empty], TaskParams(push_to=["global"]))[0].task.push_to, [])

    def test_source_routes_inherit_defaults_but_explicit_empty_overrides(self) -> None:
        config = CrawlConfig.parse(Node({
            "task": {"push_to": ["global"]},
            "pages": [{"url": "https://page.invalid"}, {"url": "https://page.invalid/empty", "push_to": []},
                      {"url": "https://page.invalid/task", "task": {"push_to": []}},
                      {"url": "https://page.invalid/custom", "push_to": ["outer"], "task": {"push_to": ["inner"]}}],
            "google": {}, "github": {"push_to": []}, "yandex": {},
            "repositories": [{"username": "fake", "repo": "fake"}, {"push_to": []}],
            "telegram": {"channels": {"fake": {}, "empty": {"push_to": []}}},
            "twitter": {"users": {"fake": {}, "empty": {"task": {"push_to": []}}}},
        }), StorageConfig())
        self.assertEqual([job.push_to for job in config.pages], [["global"], [], [], ["inner"]])
        self.assertEqual([job.task.push_to for job in config.pages], [["global"], [], [], ["inner"]])
        self.assertEqual((config.google.push_to, config.github.push_to, config.yandex.push_to), (["global"], [], ["global"]))
        self.assertEqual([item.push_to for item in config.repositories], [["global"], []])
        for mapping in (config.telegram.channels, config.twitter.users):
            self.assertEqual(mapping["fake"].push_to, ["global"])
            self.assertEqual(mapping["empty"].push_to, [])
        restored = CrawlConfig.parse(Node(config.to_dict()), StorageConfig())
        self.assertEqual([job.task.push_to for job in restored.pages], [["global"], [], [], ["inner"]])
        standalone = PageJob.parse(Node({"url": "https://page.invalid"}))
        self.assertIsNone(standalone.task.push_to)
        self.assertEqual(TaskParams(push_to=["global"]).merge(standalone.task).push_to, ["global"])

    def test_page_unrouted_diagnostic_is_redacted_and_disabled_jobs_are_silent(self) -> None:
        configs = [
            PageJob.parse(Node({"url": "https://page.invalid?token=DoNotLog"})),
            PageJob.parse(Node({"url": "https://page.invalid?token=DoNotLog", "push_to": []})),
            PageJob.parse(Node({"url": "https://page.invalid?token=DoNotLog", "enable": False})),
        ]
        with patch("crawl.helpers.fetch_jobs", return_value=ChannelResult()) as fetch_jobs, self.assertLogs(level="WARNING") as captured:
            PageChannel().crawl(configs, self.context(TaskParams(push_to=["global"])))
        jobs = fetch_jobs.call_args.args[0]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].task.push_to, ["global"])
        self.assertEqual(len(captured.output), 1)
        self.assertIn("#2", captured.output[0])
        self.assertNotIn("DoNotLog", captured.output[0])
        self.assertNotIn("https://", captured.output[0])

    def test_extract_keeps_missing_routes_distinct_from_explicit_empty(self) -> None:
        url = "https://sub.invalid/api/v1/client/subscribe?token=0123456789ABCDEF"
        task = TaskParams()
        inherited = extract_subscribes(url, task=task).items[0].task
        empty = extract_subscribes(url, push_to=[], task=task).items[0].task
        self.assertIsNone(task.push_to)
        self.assertIsNone(inherited.push_to)
        self.assertEqual(empty.push_to, [])
        self.assertEqual(TaskParams(push_to=["global"]).merge(inherited).push_to, ["global"])
        self.assertEqual(TaskParams(push_to=["global"]).merge(empty).push_to, [])

    def test_engine_can_read_cache_without_persisting_raw_nodes_or_subscriptions(self) -> None:
        url = "https://sub.invalid/synthetic"
        storage = StorageConfig(items={"cache": StorageItem(), "raw": StorageItem()})
        config = CrawlConfig(
            task=TaskParams(push_to=["global"]), pages=[PageJob(push_to=["global"])],
            persist=CrawlPersist(subscribe="cache", nodes="raw"),
        )
        for persist in (False, True):
            with self.subTest(persist=persist):
                result = ChannelResult(items=[SubItem(url, "PAGE", ready=True)])
                result.nodes.uris = ["hy2://FakeCase@[2001:db8::1]:443"]
                channel, pushtool = Mock(), Mock(spec=PushTo)
                channel.crawl.return_value = result
                with patch.dict(engine.CHANNELS, {"pages": channel}, clear=True), \
                        patch.object(engine.push, "get_instance", return_value=pushtool), \
                        patch.object(engine, "load_records", return_value={url: {"push_to": ["cached"]}}) as load, \
                        patch.object(engine, "_snapshot_nodes") as snapshot, \
                        patch.object(engine, "save_records") as save:
                    sites = engine.run(config, storage, num_threads=1, display=False, persist=persist)
                load.assert_called_once_with(pushtool, storage.items["cache"])
                self.assertEqual(len(sites), 2)
                self.assertEqual(sites[-1].nodes.uris, result.nodes.uris)
                if persist:
                    snapshot.assert_called_once()
                    save.assert_called_once()
                else:
                    snapshot.assert_not_called()
                    save.assert_not_called()
                pushtool.push_to.assert_not_called()

    def test_adjacent_quoted_and_comma_separated_uris_are_matched_individually(self) -> None:
        uris = ["trojan://FakeA@node.invalid:443", "trojan://FakeB@node.invalid:8443"]
        for content in ("['" + "','".join(uris) + "']", ",".join(uris), ";".join(uris)):
            with self.subTest(content=content):
                self.assertEqual(extract_subscribes(content, include_nodes=True).nodes.uris, uris)
        literal = "trojan://Fake'Comma,Semicolon;Bang!%27%2C@node.invalid:443/Case,Path;Part"
        self.assertEqual(extract_subscribes(f'<a href="{literal}">fake</a>', include_nodes=True).nodes.uris, [literal])

    def test_page_raw_nodes_keep_source_routes_and_transforms_in_engine(self) -> None:
        uri = "hy2://FakeCase@[2001:db8::8]:443"
        shared = TaskParams(push_to=["global"], rename="GlobalRename")
        page = PageChannel(url="https://page.invalid", push_to=["local"], task=TaskParams(rename="SourceRename"))
        with patch("utils.http_get", return_value=uri):
            result = page.fetch(self.context(shared))
        self.assertEqual(result.node_batches[0].task.push_to, ["local"])
        channel = Mock()
        channel.crawl.return_value = result
        config = CrawlConfig(task=shared, pages=[PageJob()])
        with patch.dict(engine.CHANNELS, {"pages": channel}, clear=True):
            sites = engine.run(config, num_threads=1, display=False, persist=False)
        self.assertEqual(len(sites), 1)
        self.assertEqual(sites[0].push_to, ["local"])
        self.assertEqual(sites[0].rename, "SourceRename")
        self.assertEqual(sites[0].origin, "PAGE")
        self.assertEqual(sites[0].nodes.uris, [uri])

    def test_page_fetch_explicit_empty_routes_cannot_reenable_global_raw_nodes(self) -> None:
        page = PageChannel(url="https://page.invalid?token=SyntheticPrivateNeverLog", push_to=[])
        with patch("utils.http_get", return_value="hy2://FakeCase@[2001:db8::8]:443") as fetch, self.assertLogs(level="WARNING") as captured:
            result = page.fetch(self.context(TaskParams(push_to=["global"])))
        fetch.assert_not_called()
        self.assertTrue(result.nodes.empty())
        self.assertEqual(result.node_batches, [])
        self.assertNotIn("SyntheticPrivateNeverLog", "\n".join(captured.output))

    def test_batch_merge_excludes_explicitly_unrouted_nodes_from_legacy_fallback(self) -> None:
        blocked = "hy2://FakeBlocked@[2001:db8::9]:443"
        local = "hy2://FakeLocal@[2001:db8::10]:443"
        legacy = "hy2://FakeLegacy@[2001:db8::11]:443"
        proxy = {"type": "trojan", "server": "node.invalid", "port": 443, "password": "SyntheticFake"}
        disabled = ChannelResult(nodes=NodeInput(uris=[blocked], proxies=[proxy]), node_batches=[
            NodeBatch(nodes=NodeInput(uris=[blocked], proxies=[dict(proxy)]), task=TaskParams(push_to=[]), origin="PAGE"),
        ])
        result = disabled.merge(extract_subscribes(local, push_to=["local"], include_nodes=True))
        result.nodes.uris.append(legacy)
        self.assertEqual(len(result.node_batches), 2)
        channel = Mock()
        channel.crawl.return_value = result
        config = CrawlConfig(task=TaskParams(push_to=["global"]), pages=[PageJob()])
        with patch.dict(engine.CHANNELS, {"pages": channel}, clear=True), self.assertLogs(level="WARNING"):
            sites = engine.run(config, num_threads=1, display=False, persist=False)
        self.assertEqual([site.nodes.uris for site in sites], [[local], [legacy]])
        self.assertEqual([site.push_to for site in sites], [["local"], ["global"]])
        self.assertTrue(all(not site.nodes.proxies for site in sites))

    def test_channel_exception_diagnostics_do_not_publish_url_or_token(self) -> None:
        secret_url = "https://sub.invalid/path/SyntheticSecretNeverPublish"
        channel = Mock()
        channel.crawl.side_effect = ValueError(secret_url)
        config = CrawlConfig(task=TaskParams(push_to=["global"]), pages=[PageJob()])
        with patch.dict(engine.CHANNELS, {"pages": channel}, clear=True), self.assertLogs(level="ERROR") as captured:
            self.assertEqual(engine.run(config, num_threads=1, display=False, persist=False), [])
        diagnostic = "\n".join(captured.output)
        self.assertIn("pages", diagnostic)
        self.assertIn("ValueError", diagnostic)
        self.assertNotIn(secret_url, diagnostic)
        self.assertNotIn("SyntheticSecretNeverPublish", diagnostic)

    def test_invalid_filter_diagnostics_redact_pattern_values(self) -> None:
        url = "https://sub.invalid/api/v1/client/subscribe?token=0123456789ABCDEF"
        for filters in ({"include": "SyntheticSecretNeverPublish["}, {"exclude": "SyntheticSecretNeverPublish["}):
            with self.subTest(filters=filters), self.assertLogs(level="ERROR") as captured:
                extract_subscribes(url, **filters)
            self.assertNotIn("SyntheticSecretNeverPublish", "\n".join(captured.output))

    def test_equivalent_effective_policy_merges_none_and_explicit_site_defaults(self) -> None:
        url = "https://sub.invalid/synthetic"
        implicit = SubItem(url, "PAGE", TaskParams(push_to=["implicit"], name="FirstDisplay"))
        explicit = SubItem(url, "TELEGRAM", TaskParams(
            push_to=["explicit"], name="OtherDisplay", require_tls=False, check_alive=True,
            max_rate=3.0, rename="", include="", exclude="", ignore_default_exclude=False,
        ))
        with self.assertLogs(level="WARNING"):
            variants = engine._collapse_items([implicit, explicit])
        self.assertEqual(len(variants), 1)
        self.assertEqual(variants[0].task.push_to, ["implicit", "explicit"])
        site = engine._site_from_item(variants[0], TaskParams())
        self.assertFalse(site.require_tls)
        self.assertTrue(site.check_alive)
        self.assertEqual(site.max_rate, 3.0)

    def test_distinct_effective_processing_fields_keep_separate_routes(self) -> None:
        url = "https://sub.invalid/path/SyntheticSecretNeverPublish"
        changes = {
            "require_tls": True, "check_alive": False, "max_rate": 4.0, "rename": "DifferentRewrite",
            "include": "different_include", "exclude": "different_exclude", "ignore_default_exclude": True,
        }
        for name, value in changes.items():
            with self.subTest(field=name):
                first = SubItem(url, "PAGE", TaskParams(push_to=["first"]))
                second = SubItem(url, "TELEGRAM", TaskParams(push_to=["second"], **{name: value}), ready=True)
                with self.assertLogs(level="WARNING") as captured:
                    variants = engine._collapse_items([first, second])
                self.assertEqual(len(variants), 2)
                self.assertEqual([item.task.push_to for item in variants], [["first"], ["second"]])
                diagnostic = "\n".join(captured.output)
                self.assertIn(name, diagnostic)
                self.assertNotIn("SyntheticSecretNeverPublish", diagnostic)
                self.assertNotIn(url, diagnostic)

    def test_same_url_source_variants_keep_strict_and_loose_policy_through_run(self) -> None:
        url = "https://sub.invalid/api/v1/client/subscribe?token=0123456789ABCDEF"
        shared = TaskParams(push_to=["global"], require_tls=True, check_alive=True)

        def source_results(ready: bool) -> ChannelResult:
            result = ChannelResult()
            for route, origin, task in (
                ("loose", "PAGE", TaskParams(require_tls=False, check_alive=False)),
                ("strict_a", "TELEGRAM", TaskParams(require_tls=True, check_alive=True)),
                ("strict_b", "GITHUB", TaskParams()),
            ):
                source = extract_subscribes(url, push_to=[route], task=task, source=origin)
                for item in source.items:
                    item.ready = ready
                result.merge(source)
            return result

        def assert_policies(sites: list) -> None:
            self.assertEqual(len(sites), 2)
            by_routes = {frozenset(site.push_to): site for site in sites}
            loose = by_routes[frozenset(["loose"])]
            strict = by_routes[frozenset(["strict_a", "strict_b"])]
            self.assertFalse(loose.require_tls)
            self.assertFalse(loose.check_alive)
            self.assertTrue(strict.require_tls)
            self.assertTrue(strict.check_alive)

        for ready in (False, True):
            with self.subTest(ready=ready):
                with self.assertLogs(level="WARNING"):
                    variants = engine._collapse_items(source_results(ready).items, defaults=shared)
                assert_policies([engine._site_from_item(item, shared) for item in variants])
                channel = Mock()
                channel.crawl.return_value = source_results(ready)
                config = CrawlConfig(task=shared, pages=[PageJob(push_to=["global"])])
                with patch.dict(engine.CHANNELS, {"pages": channel}, clear=True), \
                        patch.object(engine.utils, "multi_thread_run", side_effect=lambda func, tasks, **kwargs: [(True, False) for _ in tasks]) as validate, \
                        self.assertLogs(level="WARNING"):
                    sites = engine.run(config, num_threads=1, display=False, persist=False)
                assert_policies(sites)
                if ready:
                    validate.assert_not_called()
                else:
                    validate.assert_called_once()
                    self.assertEqual(len(validate.call_args.kwargs["tasks"]), 2)


if __name__ == "__main__":
    unittest.main()
