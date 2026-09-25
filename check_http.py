"""check_http.py：起服务、按脚本走一圈，打印验收面。"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request

from server import serve

WAL_PATH = "drrqueue.wal"


def call(method, url, body=None):
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:60]}


def main() -> int:
    spec = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "sample/flows.json", encoding="utf-8"))
    if os.path.exists(WAL_PATH):
        os.remove(WAL_PATH)  # 自检从干净场景开始
    server = serve(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_port
    for item in spec["packets"]:
        call("POST", base + "/enqueue", json.dumps(item).encode())
    for flow, weight in spec["weights"].items():
        call("POST", base + "/weight", json.dumps({"flow": flow, "weight": weight}).encode())
    first = parse(call("POST", base + "/dequeue", json.dumps({"rounds": spec["rounds_before"]}).encode())[1])
    call("POST", base + "/weight", json.dumps({"flow": spec["change"]["flow"],
                                              "weight": spec["change"]["weight"]}).encode())
    second = parse(call("POST", base + "/dequeue", json.dumps({"rounds": spec["rounds_after"]}).encode())[1])
    stats = parse(call("GET", base + "/stats")[1])
    recovered = parse(call("POST", base + "/recover", b"{}")[1])
    print("出队顺序 =", first.get("sent"), "+", second.get("sent"))
    print("全部出队顺序 =", stats.get("order"))
    print("每个流出队字节 =", stats.get("served"))
    print("每流欠账 =", stats.get("deficits"))
    print("公平性差值（字节/权重） =", stats.get("fairness_gap"))
    print("剩余队列长度 =", stats.get("queues"))
    print("恢复后欠账 =", recovered.get("deficits"))
    print("恢复后各流权重 =", recovered.get("weights"))
    print("不变量（出队字节 = 入队字节 - 剩余字节） =", stats.get("conserved"))
    print("每轮字节配额 =", stats.get("quota"))
    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
