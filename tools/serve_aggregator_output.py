#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
serve_aggregator_output.py
==========================

轻量 HTTP 服务器：把 aggregator000 生成的节点文件（Clash/Mihomo 格式 YAML）
通过 HTTP 暴露为订阅链接，供 subs-check 在本机通过 sub-urls 拉取。

用法:
    python tools/serve_aggregator_output.py --file output/checked/config.yaml --port 11023 --path /all.yaml

用途:
    subs-check 需要通过 sub-urls 拉取订阅，但 aggregator000 的输出在本机。
    与其依赖外部 gist raw URL（有缓存延迟且可能被限流），直接在本机起一个
    HTTP 服务暴露聚合结果，subs-check 通过 http://127.0.0.1:PORT/PATH 拉取。
"""

import argparse
import http.server
import os
import socketserver
import sys
import threading
import time
from http import HTTPStatus


class _Handler(http.server.SimpleHTTPRequestHandler):
    """只返回指定文件内容的极简处理器。"""

    serve_file: str = ""
    content_type: str = "text/plain; charset=utf-8"

    def do_GET(self):  # noqa: N802 - http.server API
        path = self.path.split("?", 1)[0].split("#", 1)[0]
        expected = os.environ.get("SERVE_PATH", "/all.yaml")
        if path not in ("/", expected):
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        if not self.serve_file or not os.path.exists(self.serve_file):
            self.send_error(HTTPStatus.NOT_FOUND, "file not found")
            return
        try:
            with open(self.serve_file, "rb") as f:
                data = f.read()
        except OSError:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "cannot read file")
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", self.content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):  # noqa: D401 - silence default logging
        sys.stderr.write("[serve] " + (fmt % args) + "\n")


class _ThreadingTCPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve a node file over HTTP for subs-check")
    parser.add_argument("--file", required=True, help="node file to serve (YAML/Clash format)")
    parser.add_argument("--port", type=int, default=11023, help="listen port (default: 11023)")
    parser.add_argument("--path", default="/all.yaml", help="request path (default: /all.yaml)")
    parser.add_argument("--content-type", default="text/plain; charset=utf-8")
    parser.add_argument("--timeout", type=int, default=0, help="auto-stop after N seconds, 0=keep running")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"[serve] file not found: {args.file}", file=sys.stderr)
        return 1

    _Handler.serve_file = os.path.abspath(args.file)
    _Handler.content_type = args.content_type
    os.environ["SERVE_PATH"] = args.path

    server = _ThreadingTCPServer(("127.0.0.1", args.port), _Handler)
    print(f"[serve] listening on http://127.0.0.1:{args.port}{args.path} -> {args.file}")

    stopper = threading.Event()

    def _shutdown():
        if stopper.is_set():
            return
        stopper.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    if args.timeout > 0:
        threading.Timer(args.timeout, _shutdown).start()

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())