"""drrqueue.py：DRR（Deficit Round Robin）出队调度。

- 每个流独立队列；set_weight 配置每轮字节配额；
- dequeue(rounds) 按轮推进：每轮按流首次入队顺序给每个流加上当前权重
  个字节的欠账，欠账够付队头包就出队（可连续多个），不够则欠账留到
  下一轮，不清零；
- 权重变化从下一轮起生效，历史欠账保留；
- 未显式设置权重的流保持旧行为（不限速），兼容老调用方；
- 队列、权重、欠账经 WAL 落盘，recover() 重放恢复，尾部半条记录忽略。
"""
from __future__ import annotations

import json
import os
from collections import deque


class DeficitQueue:
    def __init__(self, wal_path: str | None = None):
        self.queues = {}
        self.weights = {}
        self.deficits = {}
        self.served = {}
        self.order = []
        self.flow_order = []
        self.enqueued_bytes = 0
        self._weighted = set()
        self.wal_path = wal_path
        self._wal = None

    # ---- 内部 -------------------------------------------------
    def _register_flow(self, flow: str) -> None:
        if flow not in self.queues:
            self.queues[flow] = deque()
            self.weights.setdefault(flow, 1)
            self.deficits.setdefault(flow, 0)
            self.served.setdefault(flow, 0)
            self.flow_order.append(flow)

    def _log(self, records: list) -> None:
        if not self.wal_path:
            return
        if self._wal is None:
            self._wal = open(self.wal_path, "a", encoding="utf-8")
        for record in records:
            self._wal.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._wal.flush()

    def close(self) -> None:
        if self._wal is not None:
            self._wal.close()
            self._wal = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _send_head(self, flow: str, sent: list, records: list) -> int:
        packet_id, size = self.queues[flow].popleft()
        self.served[flow] += size
        self.order.append(packet_id)
        sent.append(packet_id)
        records.append({"op": "dequeue", "flow": flow, "id": packet_id, "size": size})
        return size

    def _replay(self, record: dict) -> None:
        op = record.get("op")
        if op == "enqueue":
            flow = record["flow"]
            self._register_flow(flow)
            self.queues[flow].append((record["id"], record["size"]))
            self.enqueued_bytes += record["size"]
        elif op == "dequeue":
            flow = record["flow"]
            if self.queues.get(flow):
                self.queues[flow].popleft()
                self.served[flow] += record["size"]
                self.order.append(record["id"])
        elif op == "weight":
            self.weights[record["flow"]] = record["weight"]
            self._weighted.add(record["flow"])
        elif op == "deficits":
            for flow, deficit in record["deficits"].items():
                self.deficits[flow] = deficit

    # ---- 接口 -------------------------------------------------
    def enqueue(self, flow: str, packet_id: str, size: int) -> dict:
        self._register_flow(flow)
        self.queues[flow].append((packet_id, size))
        self.enqueued_bytes += size
        self._log([{"op": "enqueue", "flow": flow, "id": packet_id, "size": size}])
        return {"depth": len(self.queues[flow])}

    def dequeue(self, rounds: int = 1) -> dict:
        sent = []
        records = []
        for _ in range(rounds):
            for flow in self.flow_order:
                queue = self.queues[flow]
                if flow not in self._weighted:
                    while queue:
                        self._send_head(flow, sent, records)
                    continue
                self.deficits[flow] += self.weights[flow]
                while queue and self.deficits[flow] >= queue[0][1]:
                    self.deficits[flow] -= self._send_head(flow, sent, records)
        records.append({"op": "deficits", "deficits": dict(self.deficits)})
        self._log(records)
        return {"sent": sent}

    def set_weight(self, flow: str, weight: int) -> dict:
        self.weights[flow] = weight
        self._weighted.add(flow)
        self._log([{"op": "weight", "flow": flow, "weight": weight}])
        return {"weight": weight}

    def recover(self) -> dict:
        self.queues = {}
        self.weights = {}
        self.deficits = {}
        self.served = {}
        self.order = []
        self.flow_order = []
        self.enqueued_bytes = 0
        self._weighted = set()
        if self.wal_path and os.path.exists(self.wal_path):
            with open(self.wal_path, "rb") as handle:
                raw = handle.read()
            lines = raw.split(b"\n")
            if raw and not raw.endswith(b"\n"):
                lines.pop()  # 尾部半条记录忽略
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue  # 坏行跳过
                self._replay(record)
        return {"queues": {flow: len(queue) for flow, queue in self.queues.items()},
                "weights": dict(self.weights),
                "deficits": dict(self.deficits)}

    def stats(self) -> dict:
        remaining = sum(size for queue in self.queues.values() for _, size in queue)
        ratios = [self.served[flow] / self.weights[flow]
                  for flow in self.flow_order
                  if flow in self._weighted and self.weights[flow]]
        gap = (max(ratios) - min(ratios)) if ratios else 0
        if gap == int(gap):
            gap = int(gap)
        return {"queues": {flow: len(queue) for flow, queue in self.queues.items()},
                "weights": dict(self.weights),
                "deficits": dict(self.deficits),
                "served": dict(self.served),
                "order": list(self.order),
                "quota": [self.weights.get(flow, 1) for flow in self.flow_order],
                "fairness_gap": gap,
                "conserved": sum(self.served.values()) == self.enqueued_bytes - remaining}
