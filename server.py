"""server.py：本机服务（基线：enqueue/dequeue/set_weight/stats）。"""
from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from drrqueue import DeficitQueue

QUEUE = DeficitQueue()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(QUEUE.stats()).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/enqueue"):
            result = QUEUE.enqueue(payload["flow"], payload["id"], payload["size"])
        elif self.path.startswith("/dequeue"):
            result = QUEUE.dequeue(payload.get("rounds", 1))
        elif self.path.startswith("/weight"):
            result = QUEUE.set_weight(payload["flow"], payload["weight"])
        elif self.path.startswith("/recover"):
            result = QUEUE.recover()
        else:
            result = {}
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def serve(port: int = 0):
    return HTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print("listening on http://127.0.0.1:%d" % port)
    serve(port).serve_forever()
