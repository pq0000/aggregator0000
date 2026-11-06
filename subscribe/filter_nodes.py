# -*- coding: utf-8 -*-
"""Filter nodes by protocol and push to Gist."""
import json
import os
import sys
import urllib.request
import ssl
import yaml

GIST_ID = "942e3c9de1396e6b3bf4afab0f054349"
TARGET = {"vless", "hysteria2", "mieru", "masque"}
OUTPUT = "filtered_nodes.yaml"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def load_nodes():
    for path in ["output/checked/all.yaml", "output/all.yaml"]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                if isinstance(data, dict) and "proxies" in data:
                    return data["proxies"]
            except Exception as e:
                print("[Filter] load error:", e)
    return []


def push(content):
    token = os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("[Filter] no token")
        return False
    url = "https://api.github.com/gists/" + GIST_ID
    body = json.dumps({"files": {"filtered_nodes.yaml": {"content": content}}}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="PATCH",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aggregator-filter",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            print("[Filter] pushed, status", r.status)
            return r.status == 200
    except Exception as e:
        print("[Filter] push error:", e)
        return False


def main():
    nodes = load_nodes()
    print("[Filter] total", len(nodes))
    filtered = []
    seen = set()
    for n in nodes:
        t = str(n.get("type", "")).lower()
        if t in TARGET:
            key = (t, n.get("server"), n.get("port"))
            if key in seen:
                continue
            seen.add(key)
            filtered.append(n)
    print("[Filter] filtered", len(filtered))
    if not filtered:
        push("# no vless/hysteria2/mieru/masque nodes")
        sys.exit(0)
    text = yaml.dump({"proxies": filtered}, allow_unicode=True, default_flow_style=False)
    if push(text):
        print("[Filter] done")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()