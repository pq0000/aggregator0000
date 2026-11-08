#!/usr/bin/env python3
"""Filter a Mihomo YAML file to the protocols used by the pipeline."""
import argparse
import re
from pathlib import Path

import yaml

TARGET_PROTOCOLS = {"vless", "hysteria2", "mieru", "masque"}


def filter_file(source: Path, destination: Path) -> int:
    raw = source.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(re.sub(r"!<\w+>", "", raw))
    except yaml.YAMLError as error:
        raise ValueError(f"invalid aggregate YAML: {error}") from error

    if not isinstance(data, dict) or not isinstance(data.get("proxies"), list):
        raise ValueError("aggregate must contain a proxies list")
    nodes = data["proxies"]
    if not nodes or any(not isinstance(node, dict) for node in nodes):
        raise ValueError("aggregate must contain non-empty proxy objects")

    kept = [node for node in nodes if str(node.get("type", "")).lower() in TARGET_PROTOCOLS]
    if not kept:
        raise ValueError("no nodes matched target protocols; refusing to publish empty output")

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        yaml.safe_dump({"proxies": kept}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    print(f"[filter] input={len(nodes)}, kept={len(kept)}")
    return len(kept)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        filter_file(args.source, args.destination)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
