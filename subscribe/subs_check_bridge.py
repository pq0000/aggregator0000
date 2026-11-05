# -*- coding: utf-8 -*-
"""
subs-check 联动脚本
将 aggregator000 的采集结果转换为 subs-check 可接受的输入格式，
并在检测完成后将结果整理为 Mihomo 订阅文件。
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml

PATH = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(PATH, ".."))
OUTPUT_DIR = os.path.join(ROOT, "output")
RAW_DIR = os.path.join(OUTPUT_DIR, "raw")
CHECKED_DIR = os.path.join(OUTPUT_DIR, "checked")
REPORTS_DIR = os.path.join(OUTPUT_DIR, "reports")

# ============================================================
# 协议分类正则
# ============================================================
PROTOCOL_PATTERNS = {
    "vless": re.compile(r"^vless://", re.I),
    "hysteria2": re.compile(r"^hysteria2://|^hy2://", re.I),
    "hysteria": re.compile(r"^hysteria://", re.I),
    "tuic": re.compile(r"^tuic://", re.I),
    "vmess": re.compile(r"^vmess://", re.I),
    "trojan": re.compile(r"^trojan://", re.I),
    "ss": re.compile(r"^ss://", re.I),
    "ssr": re.compile(r"^ssr://", re.I),
    "mieru": re.compile(r"^mieru://", re.I),
    "masque": re.compile(r"^masque://", re.I),
    "http": re.compile(r"^http://", re.I),
    "socks": re.compile(r"^socks://|socks4://|socks5://", re.I),
}


def ensure_dirs():
    """确保输出目录存在"""
    for d in [RAW_DIR, CHECKED_DIR, REPORTS_DIR]:
        os.makedirs(d, exist_ok=True)


def classify_protocol(uri: str) -> str:
    """根据 URI 分类协议类型"""
    for proto, pattern in PROTOCOL_PATTERNS.items():
        if pattern.match(uri):
            return proto
    return "unknown"


def load_raw_nodes(raw_file: str) -> list[str]:
    """从 aggregator000 的原始节点文件中加载节点 URI"""
    filepath = os.path.join(RAW_DIR, raw_file)
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read().strip()
    if not content:
        return []
    # 可能是 base64 编码
    if re.match(r"^(vless|trojan|ss|vmess|hysteria|tuic)://", content, re.I):
        return [line for line in content.split("\n") if line.strip()]
    try:
        import base64
        decoded = base64.b64decode(content).decode("utf-8")
        return [line for line in decoded.split("\n") if line.strip()]
    except Exception:
        return [line for line in content.split("\n") if line.strip()]


def load_crawled_subs(subs_file: str) -> list[str]:
    """从爬取的订阅链接文件中加载订阅 URL"""
    filepath = os.path.join(RAW_DIR, subs_file)
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            urls = []
            for k, v in data.items():
                if isinstance(v, dict) and "url" in v:
                    urls.append(v["url"])
                elif isinstance(v, str):
                    urls.append(v)
            return urls
        except json.JSONDecodeError:
            return [line.strip() for line in f if line.strip()]


def build_subs_check_input(
    nodes_file: str = "crawledproxies.txt",
    subs_file: str = "crawledsubs.json",
    output_file: str = "subs_check_input.yaml",
) -> str:
    """构建 subs-check 输入文件（YAML 格式订阅列表）"""
    ensure_dirs()
    node_uris = load_raw_nodes(nodes_file)
    sub_urls = load_crawled_subs(subs_file)

    input_data = {"sub-urls": []}

    # 添加订阅链接
    for url in sub_urls:
        input_data["sub-urls"].append(url)

    # 添加直接节点 URI
    if node_uris:
        node_content = "\n".join(node_uris)
        import base64
        encoded = base64.b64encode(node_content.encode("utf-8")).decode("utf-8")
        # 将节点 URI 写入临时订阅文件供 subs-check 拉取
        nodes_sub_file = os.path.join(RAW_DIR, "aggregator-nodes.txt")
        with open(nodes_sub_file, "w", encoding="utf-8") as f:
            f.write(node_content)
        input_data["sub-urls"].append(f"file://{nodes_sub_file}#aggregator-nodes")

    output_path = os.path.join(OUTPUT_DIR, output_file)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(input_data, f, allow_unicode=True, default_flow_style=False)

    print(f"[Bridge] subs-check input written to: {output_path}")
    print(f"  - Subscription URLs: {len(sub_urls)}")
    print(f"  - Direct nodes: {len(node_uris)}")
    return output_path


def run_subs_check(config_path: str, binary_path: str = "") -> bool:
    """运行 subs-check"""
    ensure_dirs()

    if not binary_path:
        # 尝试查找 subs-check 二进制
        candidates = [
            os.path.join(ROOT, "subs-check"),
            os.path.join(ROOT, "subs-check.exe"),
            "subs-check",
        ]
        for c in candidates:
            if os.path.exists(c):
                binary_path = c
                break
        if not binary_path or not os.path.exists(binary_path):
            print("[Bridge] subs-check binary not found, please specify --binary")
            return False

    cmd = [binary_path, "-f", config_path]
    print(f"[Bridge] running subs-check: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        print(result.stdout[-2000:] if result.stdout else "")
        if result.stderr:
            print(f"[Bridge stderr] {result.stderr[-1000:]}")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("[Bridge] subs-check timed out")
        return False
    except Exception as e:
        print(f"[Bridge] subs-check error: {e}")
        return False


def parse_subs_check_output(report_file: str = "results.json") -> dict:
    """解析 subs-check 输出报告"""
    filepath = os.path.join(OUTPUT_DIR, "cache", report_file)
    if not os.path.exists(filepath):
        return {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def generate_mihomo_output(check_result: dict, timestamp: str) -> dict[str, list[dict]]:
    """从 subs-check 结果生成 Mihomo 格式的节点分组"""
    nodes = check_result.get("nodes", [])
    if not nodes:
        return {}

    groups = {"vless": [], "hysteria2": [], "other": []}
    for node in nodes:
        node_type = node.get("type", "unknown")
        node_data = {
            "name": node.get("name", ""),
            "type": node_type,
            "server": node.get("server", ""),
            "port": node.get("port", ""),
            "client_fingerprint": node.get("client-fingerprint", node.get("fingerprint", "")),
        }
        # 复制所有 mihomo 兼容字段
        for key in ["uuid", "password", "uuid", "servername", "sni", "network", "tls",
                     "reality-opts", "ws-opts", "grpc-opts", "h2-opts", "http-opts",
                     "plugin", "obfs", "obfs-password", "port-range", "username",
                     "multiplexing", "handshake-mode", "auth_str", "auth-str",
                     "up", "down", "ca", "ca-str", "skip-cert-verify", "disabled"]:
            if key in node and node[key] not in (None, ""):
                node_data[key] = node[key]

        # 分类
        if node_type == "vless":
            groups["vless"].append(node_data)
        elif node_type == "hysteria2":
            groups["hysteria2"].append(node_data)
        else:
            groups["other"].append(node_data)

    return groups


def write_mihomo_yaml(groups: dict[str, list[dict]], output_dir: str):
    """写入 Mihomo YAML 配置文件"""
    ensure_dirs()

    # 全部节点
    all_proxies = []
    for group_nodes in groups.values():
        all_proxies.extend(group_nodes)

    if all_proxies:
        # 添加 metadata
        for p in all_proxies:
            p["fallback"] = True

        config = {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "Rule",
            "log-level": "silent",
            "proxies": all_proxies,
            "proxy-groups": [
                {
                    "name": "🚀 节点选择",
                    "type": "select",
                    "proxies": ["自动选择", "直接连接"],
                },
                {
                    "name": "自动选择",
                    "type": "url-test",
                    "proxies": list(g["name"] for g in all_proxies),
                    "url": "https://www.gstatic.com/generate_204",
                    "interval": 300,
                },
                {
                    "name": "🇨🇳 国内直连",
                    "type": "select",
                    "proxies": ["直接连接"],
                },
                {
                    "name": "直接连接",
                    "type": "direct",
                },
            ],
            "rules": [
                "GEOSITE,cn,🇨🇳 国内直连",
                "GEOIP,cn,🇨🇳 国内直连,no-resolve",
                "MATCH,🚀 节点选择",
            ],
        }

        filepath = os.path.join(output_dir, "all.yaml")
        with open(filepath, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        print(f"[Bridge] wrote Mihomo config: {filepath}")

    # 按协议分类
    for proto, nodes in groups.items():
        if not nodes:
            continue
        config = {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "Rule",
            "log-level": "silent",
            "proxies": nodes,
            "proxy-groups": [
                {
                    "name": "🚀 节点选择",
                    "type": "select",
                    "proxies": ["自动选择", "直接连接"],
                },
                {
                    "name": "自动选择",
                    "type": "url-test",
                    "proxies": [g["name"] for g in nodes],
                    "url": "https://www.gstatic.com/generate_204",
                    "interval": 300,
                },
                {"name": "直接连接", "type": "direct"},
            ],
            "rules": ["GEOSITE,cn,直接连接", "GEOIP,cn,直接连接,no-resolve", "MATCH,🚀 节点选择"],
        }
        filepath = os.path.join(output_dir, f"{proto}.yaml")
        with open(filepath, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        print(f"[Bridge] wrote {proto} config: {filepath}")


def main():
    parser = argparse.ArgumentParser(description="aggregator000 ↔ subs-check 联动桥接脚本")
    parser.add_argument("--build-input", action="store_true", help="构建 subs-check 输入文件")
    parser.add_argument("--run-check", action="store_true", help="运行 subs-check 检测")
    parser.add_argument("--generate-output", action="store_true", help="从检测结果生成 Mihomo 输出")
    parser.add_argument("--full-pipeline", action="store_true", help="运行完整流程（构建→检测→输出）")
    parser.add_argument("--binary", default="", help="subs-check 二进制路径")
    parser.add_argument("--config", default="", help="subs-check 配置文件路径")
    parser.add_argument("--input-file", default="subs_check_input.yaml", help="输入文件路径")
    parser.add_argument("--report-file", default="results.json", help="结果报告文件名")
    parser.add_argument("--raw-nodes", default="crawledproxies.txt", help="原始节点文件名")
    parser.add_argument("--raw-subs", default="crawledsubs.json", help="原始订阅链接文件名")
    args = parser.parse_args()

    ensure_dirs()
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.full_pipeline or args.build_input:
        build_subs_check_input(
            nodes_file=args.raw_nodes,
            subs_file=args.raw_subs,
            output_file=args.input_file,
        )

    if args.full_pipeline or args.run_check:
        config_path = args.config or os.path.join(ROOT, "config", "subs-check.local.yaml")
        if run_subs_check(config_path, args.binary):
            print("[Bridge] subs-check completed successfully")
        else:
            print("[Bridge] subs-check failed or was skipped")

    if args.full_pipeline or args.generate_output:
        result = parse_subs_check_output(args.report_file)
        if result:
            groups = generate_mihomo_output(result, now)
            write_mihomo_yaml(groups, CHECKED_DIR)
            # 保存报告
            report_path = os.path.join(REPORTS_DIR, f"report-{now}.json")
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"[Bridge] report saved: {report_path}")
        else:
            print("[Bridge] no subs-check result found, skipping output generation")

    print("[Bridge] done")


if __name__ == "__main__":
    main()
