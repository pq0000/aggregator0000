# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import sys
import urllib.parse

import utils
from config.models import NodeInput, TaskParams
from crawl.models import ChannelResult, NodeBatch, SubItem
from logger import logger
from origin import Origin

SUB_REGEX = r"https?://(?:[a-zA-Z0-9\u4e00-\u9fa5\-]+\.)+[a-zA-Z0-9\u4e00-\u9fa5\-]+(?::\d+)?(?:(?:(?:/index.php)?/api/v1/client/subscribe\?token=[a-zA-Z0-9]{16,32})|(?:/link/[a-zA-Z0-9]+\?(?:sub|mu|clash)=\d)|(?:/(?:s|sub)/[a-zA-Z0-9]{32}))|https://jmssub\.net/members/getsub\.php\?service=\d+&id=[a-zA-Z0-9\-]{36}(?:\S+)?"
EXTRA_REGEX = r"https?://(?:[a-zA-Z0-9\u4e00-\u9fa5\-]+\.)+[a-zA-Z0-9\u4e00-\u9fa5\-]+/sub\?(?:\S+)?target=\S+"
# URI schemes are case-insensitive; their credentials, paths and encoded payloads are not.
_PROTOCOL_SCHEMES = r"(?:vmess|trojan|ss|ssr|snell|hysteria2|hy2|vless|hysteria|tuic|anytls|mieru|mierus|masque)"
# A comma/semicolon before another URI is a list boundary, not part of a password.
# Single-quoted containers end at their quote; raw apostrophes remain legal in a
# bare URI or a double-quoted container. Apply these boundaries while matching,
# so adjacent links do not disappear inside one greedy candidate.
_URI_SEPARATOR = rf"[,;]['\"]?{_PROTOCOL_SCHEMES}://"
PROTOCOL_REGEX = (
    rf"(?<![A-Za-z0-9+._-])(?:"
    rf"(?<='){_PROTOCOL_SCHEMES}://(?:(?!{_URI_SEPARATOR})[A-Za-z0-9:/?#\[\]@!$&()*+,;=%._~+-])+"
    rf"|(?<!'){_PROTOCOL_SCHEMES}://(?:(?!{_URI_SEPARATOR})[A-Za-z0-9:/?#\[\]@!$&'()*+,;=%._~+-])+"
    r")"
)
_BASE64_SCHEMES = {"vmess", "ss", "ssr"}


def _extract_node_uris(content: str) -> list[str]:
    uris = []
    for match in re.finditer(PROTOCOL_REGEX, content, flags=re.I):
        uri = match.group(0)
        before = content[match.start() - 1] if match.start() else ""
        if before in ("(", "[") and uri.endswith(")" if before == "(" else "]"):
            # Remove the enclosing Markdown/prose delimiter, not URI-internal punctuation.
            uri = uri[:-1]

        scheme, payload = uri.split("://", 1)
        if scheme.lower() in _BASE64_SCHEMES and "@" not in payload:
            encoded = re.match(r"[A-Za-z0-9+/_=-]+", payload)
            if not encoded:
                continue
            end = encoded.end()
            # Base64 cannot contain sentence punctuation; keep a URI fragment verbatim.
            uri = uri[: len(scheme) + 3 + end] + (payload[end:] if payload[end:].startswith("#") else "")
        else:
            try:
                parts = urllib.parse.urlsplit(uri)
                if not parts.hostname:
                    continue
                # Punctuation immediately following an endpoint is not a port. Do not
                # strip punctuation from userinfo, a path, a query, or a fragment.
                authority = re.fullmatch(r"(.+:\d+)([.,;:!?')\]]+)", parts.netloc)
                if authority and not parts.path and not parts.query and not parts.fragment:
                    uri = uri[: len(scheme) + 3] + authority.group(1)
                    parts = urllib.parse.urlsplit(uri)
                port = parts.port
                if port is not None and not 0 < port <= 65535:
                    continue
            except ValueError:
                continue
        uris.append(uri)
    return uris


def extract_subscribes(
    content: str,
    push_to: list[str] | None = None,
    include: str = "",
    exclude: str = "",
    limits: int = sys.maxsize,
    source: str = Origin.OWNED.name,
    task: TaskParams | None = None,
    reversed: bool = False,
    skip_cache: bool = False,
    include_nodes: bool = False,
) -> ChannelResult:
    result = ChannelResult()
    if not content:
        return result

    routes = list(push_to) if push_to is not None else None
    defaults = TaskParams(push_to=routes)
    task = defaults.merge(task) if task is not None else defaults

    try:
        limits = max(1, limits)
        pattern = f"{SUB_REGEX}|{EXTRA_REGEX}"
        if include:
            try:
                pattern = f"{pattern}|{include}" if include.startswith("|") else f"{pattern}|{include}"
                if not include.startswith("|"):
                    pattern = f"{SUB_REGEX}|{EXTRA_REGEX}|{include}"
                else:
                    pattern = f"{SUB_REGEX}|{EXTRA_REGEX}{include}"
                found = re.findall(pattern, content, flags=re.I)
            except Exception:
                logger.error("[ExtractError] invalid include pattern (value redacted)")
                found = re.findall(f"{SUB_REGEX}|{EXTRA_REGEX}", content)
        else:
            found = re.findall(f"{SUB_REGEX}|{EXTRA_REGEX}", content, flags=re.I)

        try:
            parts = re.findall(
                r"(?m)^#(?:\s+)?(?:!MANAGED-CONFIG|订阅链接)[^\n]*?(https?://[^\s\"'<>]+)",
                content,
                flags=re.I,
            )
            if parts:
                found.extend([utils.trim(item) for item in parts])
        except Exception:
            pass

        if reversed:
            found.reverse()

        seen = []
        uris = []
        for sub in found:
            items = [sub]
            if "url=" in sub:
                query = urllib.parse.urlparse(sub.replace("&amp;", "&")).query
                items = []
                # Unlike form fields, URI values may contain a literal '+' in Base64
                # or credentials. Decode percent escapes without changing it to a space.
                urls = [
                    urllib.parse.unquote(value)
                    for part in query.split("&")
                    for key, sep, value in [part.partition("=")]
                    if sep and urllib.parse.unquote(key) == "url"
                ]
                if not urls:
                    continue
                for url in urls:
                    if not utils.isurl(url):
                        if include_nodes:
                            uris.extend(_extract_node_uris(url))
                        continue
                    items.extend([x for x in url.split("|") if not re.match(EXTRA_REGEX, x, flags=re.I)])

            for link in items:
                link = re.sub(r"\\/|\/", "/", link, flags=re.I)
                try:
                    if include and not re.match(
                        r"https?://(?:[a-zA-Z0-9\u4e00-\u9fa5\-]+\.)+[a-zA-Z0-9\u4e00-\u9fa5\-]+.*",
                        link,
                        flags=re.I,
                    ):
                        continue
                    if exclude and re.search(exclude, link):
                        continue
                except Exception:
                    logger.error("[ExtractError] invalid include/exclude pattern (values redacted)")

                if utils.is_suspicious_url(link):
                    continue
                if link in seen:
                    continue
                seen.append(link)
                result.items.append(
                    SubItem(
                        url=link,
                        origin=source,
                        task=task,
                        skip_cache=skip_cache,
                    )
                )
                if len(seen) >= limits:
                    break
            if len(seen) >= limits:
                break

        if include_nodes:
            try:
                uris.extend(_extract_node_uris(content))
            except Exception:
                logger.error("[ExtractError] failed to extract single proxy")

        result.nodes = NodeInput(subscribe=seen, uris=list(dict.fromkeys(uris)))
        if result.nodes.uris:
            result.node_batches.append(
                NodeBatch(nodes=NodeInput(uris=list(result.nodes.uris)), task=task, origin=source)
            )
        return result
    except Exception:
        logger.error("[ExtractError] extract subscribe error")
        return ChannelResult()
