#!/usr/bin/env python3
"""Serve one immutable current-round subscription snapshot on loopback only.

Read the file once, never serve a directory or a later replacement. The private
ready file proves which child and SHA256 are serving it. Access logs deliberately
omit request paths, payloads, names and credentials.
"""
from __future__ import annotations

import argparse
import hashlib
import http.server
import json
import os
import signal
import sys
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit


class _Handler(http.server.BaseHTTPRequestHandler):
    data = b""
    expected_path = "/all.yaml"
    content_type = "text/plain; charset=utf-8"

    def _serve(self, body):
        if urlsplit(self.path).path != self.expected_path:
            self.send_error(404, "not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", self.content_type)
        self.send_header("Content-Length", str(len(self.data)))
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("X-Content-SHA256", hashlib.sha256(self.data).hexdigest())
        self.end_headers()
        if body:
            try:
                self.wfile.write(self.data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_GET(self):  # noqa: N802
        self._serve(True)

    def do_HEAD(self):  # noqa: N802
        self._serve(False)

    def log_message(self, fmt, *args):
        # Default logging includes arbitrary query strings; do not expose them.
        pass


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _ready(path: Path, payload: dict) -> None:
    path = path.absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.exists():
        raise ValueError("ready path must be new")
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".ready-", delete=False) as stream:
        stream.write(json.dumps(payload).encode())
        name = stream.name
    os.replace(name, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--port", type=int, default=11023)
    parser.add_argument("--path", default="/all.yaml")
    parser.add_argument("--content-type", default="text/plain; charset=utf-8")
    parser.add_argument("--timeout", type=int, default=0)
    parser.add_argument("--ready-file", type=Path)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not args.path.startswith("/") or any(char in args.path for char in "?#\r\n") or args.timeout < 0:
        parser.error("invalid port, exact route or timeout")
    try:
        if not args.file.is_file() or args.file.is_symlink():
            raise ValueError("regular subscription file required")
        data = args.file.read_bytes()
        if not data:
            raise ValueError("nonempty subscription required")
        _Handler.data, _Handler.expected_path, _Handler.content_type = data, args.path, args.content_type
        server = _Server(("127.0.0.1", args.port), _Handler)
        if args.ready_file:
            _ready(args.ready_file, {"pid": os.getpid(), "sha256": hashlib.sha256(data).hexdigest(), "port": args.port})
    except (OSError, ValueError):
        print("[serve] startup_failed", file=sys.stderr)
        return 1
    print(f"[serve] loopback_only=true bytes={len(data)}", flush=True)
    timer = None
    stopped = threading.Event()

    def shutdown(*_):
        if not stopped.is_set():
            stopped.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    if args.timeout:
        timer = threading.Timer(args.timeout, shutdown)
        timer.daemon = True
        timer.start()
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        if timer:
            timer.cancel()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
