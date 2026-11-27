# -*- coding: utf-8 -*-
"""Previous-round seed cache: re-inject the last published product as a source.

The published Gist's ``all.yaml`` holds the previous round's fully checked
nodes. Reading it back at collection time and merging it as one more source
lets still-alive nodes re-enter this round's pool. Seeds get NO exemption:
they pass through the same protocol filter, dedup, alive/speed checks and
HTTPS recheck as every other candidate, and they roll over for a single
round only (this round's publication replaces the Gist content they were
read from, so a node that stops passing checks drops out on its own).

Fail-open by design: any problem (no credential, network error, malformed
document, missing or empty product) yields an empty seed list plus a status
string, and collection proceeds without it.
"""
from __future__ import annotations

import json
import os
import urllib.request

import yaml

SEED_SOURCE_ID = "source-seed-previous-round"
DEFAULT_GIST_ID = "942e3c9de1396e6b3bf4afab0f054349"
SEED_FILE = "all.yaml"
API_BASE = "https://api.github.com"


def _gist_document(gist_id: str, token: str, opener, timeout: int) -> dict:
    request = urllib.request.Request(
        f"{API_BASE}/gists/{gist_id}",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28",
                 "User-Agent": "aggregator-seed-cache"},
    )
    with opener(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _file_content(entry: dict, token: str, opener, timeout: int) -> str:
    content = entry.get("content")
    if isinstance(content, str) and not entry.get("truncated"):
        return content
    raw_url = entry.get("raw_url")
    if not isinstance(raw_url, str) or not raw_url:
        return content if isinstance(content, str) else ""
    request = urllib.request.Request(
        raw_url,
        headers={"Authorization": f"Bearer {token}",
                 "User-Agent": "aggregator-seed-cache"},
    )
    with opener(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def fetch_seed_nodes(gist_id: str | None = None, token: str | None = None,
                     opener=None, timeout: int = 30) -> tuple[list[dict], str]:
    """Return (seed proxy dicts, status token). Never raises.

    Status tokens: ``ok``, ``no_credentials``, ``fetch_failed``,
    ``seed_file_missing``, ``seed_empty``.
    """
    gist_id = (gist_id or os.environ.get("GIST_ID") or DEFAULT_GIST_ID).strip()
    if token is None:
        token = os.environ.get("PUSH_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not token:
        return [], "no_credentials"
    opener = opener or urllib.request.urlopen
    try:
        document = _gist_document(gist_id, token, opener, timeout)
        files = document.get("files") if isinstance(document, dict) else None
        entry = files.get(SEED_FILE) if isinstance(files, dict) else None
        if not isinstance(entry, dict):
            return [], "seed_file_missing"
        text = _file_content(entry, token, opener, timeout)
        payload = yaml.safe_load(text) if text else None
    except Exception:
        return [], "fetch_failed"
    if not isinstance(payload, dict):
        return [], "seed_empty"
    proxies = payload.get("proxies")
    if not isinstance(proxies, list):
        return [], "seed_empty"
    nodes = [node for node in proxies if isinstance(node, dict)]
    if not nodes:
        return [], "seed_empty"
    return nodes, "ok"
