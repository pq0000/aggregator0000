# -*- coding: utf-8 -*-
"""Filter nodes by protocol (vless/hysteria2/mieru/masque) and push to Gist.
Reads all.yaml from Gist raw URL (avoids GitHub API content truncation)."""
import json
import os
import sys
import urllib.request
import ssl
import yaml

GIST_ID = "942e3c9de1396e6b3bf4afab0f054349"
OWNER = "pq0000"
RAW_URL = f"https://gist.githubusercontent.com/{OWNER}/{GIST_ID}/raw/all.yaml"
TARGET = {"vless", "hysteria2", "mieru", "masque"}
OUTPUT = "filtered_nodes.yaml"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def get_token():
    return os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN")


def download_all_yaml():
    """Download full all.yaml from raw Gist URL."""
    token = get_token()
    headers = {"User-Agent": "Mozilla/5.0"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(RAW_URL, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print("[Filter] download error:", e)
        return ""


def parse_nodes(content):
    try:
        data = yaml.safe_load(content)
        if isinstance(data, dict) and "proxies" in data:
            return data["proxies"]
        return []
    except Exception as e:
        print("[Filter] yaml parse error:", e)
        return []


def push_gist(content):
    token = get_token()
    if not token:
        print("[Filter] no token")
        return False
    url = "https://api.github.com/gists/" + GIST_ID
    payload = {"files": {OUTPUT: {"content": content}}}
    body = json.dumps(payload).encode("utf-8")
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
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            print("[Filter] pushed, status", r.status)
            return r.status == 200
    except Exception as e:
        print("[Filter] push error:", e)
        return False


def main():
    content = download_all_yaml()
    if not content:
        print("[Filter] no content downloaded")
        sys.exit(1)
    nodes = parse_nodes(content)
    print("[Filter] total nodes:", len(nodes))

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

    print("[Filter] filtered (vless/hy2/mieru/masque):", len(filtered))
    if not filtered:
        print("[Filter] no target nodes found")
        push_gist("# no vless/hysteria2/mieru/masque nodes")
        sys.exit(0)

    text = yaml.dump({"proxies": filtered}, allow_unicode=True, default_flow_style=False)
    if push_gist(text):
        print("[Filter] done")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()