"""Small, versioned registry of deliberately selected public subscription feeds."""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from config.models import SiteConfig
from config.parse import Node


PUBLIC_HOSTS = frozenset({"raw.githubusercontent.com", "gist.githubusercontent.com"})


def load_public_sources(filename: str, groups: list[str]) -> list[SiteConfig]:
    payload = json.loads(Path(filename).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("public source registry must use schema_version 1")
    records = payload.get("sources")
    if not isinstance(records, list) or len(records) > 20:
        raise ValueError("public source registry must contain at most 20 sources")
    sites, seen = [], set()
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("public source records must be objects")
        source_id = record.get("id", "")
        if not isinstance(source_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,48}", source_id):
            raise ValueError("invalid public source id")
        if source_id in seen:
            raise ValueError("duplicate public source id")
        seen.add(source_id)
        enabled = record.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("public source enabled must be boolean")
        if not enabled:
            continue
        url = record.get("url", "")
        if not isinstance(url, str):
            raise ValueError("public source URL must be text")
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.hostname not in PUBLIC_HOSTS or
                parsed.username or parsed.password or parsed.query or parsed.fragment or
                parsed.port not in (None, 443) or not parsed.path):
            raise ValueError("public registry accepts only credential-free HTTPS GitHub raw URLs")
        routes = record.get("push_to", groups)
        if not isinstance(routes, list) or not routes or any(route not in groups for route in routes):
            raise ValueError("public source must route to existing output groups")
        sites.append(SiteConfig.parse(Node({"name": f"public-{source_id}", "subscribe": [url],
                         "enable": True, "push_to": list(dict.fromkeys(routes)),
                         "check_alive": True, "ignore_default_exclude": True})))
    return sites
