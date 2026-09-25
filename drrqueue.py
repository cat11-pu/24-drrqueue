"""drrqueue.py：赤字轮询（DRR）出队调度，权重生效，欠账跨轮携带，WAL 落盘可恢复。"""
from __future__ import annotations

import json
import os
from collections import deque

DEFAULT_WEIGHT = 1500  # 未设权重时的默认每轮字节配额（MTU 量级，DRR 惯例）


class DeficitQueue:
    def __init__(self, wal_path: str | None = None):
        self.wal_path = wal_path
        self._wal = open(wal_path, "a", encoding="utf-8") if wal_path else None
        self._reset()

    def _reset(self):
        self.queues = {}          # flow -> deque[(packet_id, size)]
        self.weights = {}         # flow -> 每轮字节配额
        self.deficits = {}        # flow -> 欠账（跨轮携带，不清零）
        self.served = {}          # flow -> 已出队字节
        self.flow_order = []      # 流首次入队的顺序
        self.order = []           # 全部出队顺序
        self.enqueued_bytes = 0

    # ---- 内部：状态变更（不落盘，供 enqueue/dequeue/set_weight/recover 复用） ----

    def _apply_enqueue(self, flow: str, packet_id: str, size: int):
        if flow not in self.queues:
            self.queues[flow] = deque()
            self.weights[flow] = DEFAULT_WEIGHT
            self.deficits[flow] = 0
            self.served[flow] = 0
            self.flow_order.append(flow)
        self.queues[flow].append((packet_id, size))
        self.enqueued_bytes += size

    def _apply_weight(self, flow: str, weight: int):
        if flow not in self.weights:
            self.queues[flow] = deque()
            self.deficits[flow] = 0
            self.served[flow] = 0
            self.flow_order.append(flow)
        self.weights[flow] = weight

    def _apply_dequeue(self, rounds: int) -> list:
        sent = []
        for _ in range(rounds):
            for flow in self.flow_order:  # 固定顺序，不重排
                self.deficits[flow] += self.weights[flow]
                queue = self.queues[flow]
                while queue and queue[0][1] <= self.deficits[flow]:
                    packet_id, size = queue.popleft()
                    self.deficits[flow] -= size
                    self.served[flow] += size
                    self.order.append(packet_id)
                    sent.append(packet_id)
        return sent

    def _log(self, record: dict):
        if self._wal:
            self._wal.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._wal.flush()

    # ---- 对外接口（返回值保持兼容） ----

    def enqueue(self, flow: str, packet_id: str, size: int) -> dict:
        self._apply_enqueue(flow, packet_id, size)
        self._log({"op": "enqueue", "flow": flow, "id": packet_id, "size": size})
        return {"depth": len(self.queues[flow])}

    def dequeue(self, rounds: int = 1) -> dict:
        sent = self._apply_dequeue(rounds)
        self._log({"op": "dequeue", "rounds": rounds})
        return {"sent": sent}

    def set_weight(self, flow: str, weight: int) -> dict:
        self._apply_weight(flow, weight)
        self._log({"op": "weight", "flow": flow, "weight": weight})
        return {"weight": weight}

    def recover(self) -> dict:
        """从 WAL 重放恢复队列、权重、欠账；尾部半条记录忽略。"""
        if not self.wal_path:
            raise RuntimeError("未配置 WAL 路径，无法恢复")
        self._reset()
        if os.path.exists(self.wal_path):
            with open(self.wal_path, encoding="utf-8") as wal:
                for line in wal:
                    try:
                        record = json.loads(line)
                    except ValueError:
                        break  # 尾部半条记录，忽略
                    op = record.get("op")
                    if op == "enqueue":
                        self._apply_enqueue(record["flow"], record["id"], record["size"])
                    elif op == "weight":
                        self._apply_weight(record["flow"], record["weight"])
                    elif op == "dequeue":
                        self._apply_dequeue(record["rounds"])
        return self.stats()

    def stats(self) -> dict:
        remaining = sum(size for items in self.queues.values() for _, size in items)
        ratios = [self.served[f] / self.weights[f] for f in self.flow_order if self.weights[f]]
        gap = (max(ratios) - min(ratios)) if ratios else 0
        if gap == int(gap):
            gap = int(gap)
        return {"queues": {flow: len(items) for flow, items in self.queues.items()},
                "weights": dict(self.weights), "deficits": dict(self.deficits),
                "served": dict(self.served), "order": list(self.order),
                "quanta": [self.weights[f] for f in self.flow_order],
                "fairness_gap": gap,
                "conserved": sum(self.served.values()) == self.enqueued_bytes - remaining}
