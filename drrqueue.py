"""drrqueue.py：出队调度（基线：先来先出，权重不生效）。"""
from __future__ import annotations


class DeficitQueue:
    def __init__(self):
        self.queues = {}
        self.weights = {}
        self.deficits = {}
        self.served = {}
        self.order = []
        self.wal = []

    def enqueue(self, flow: str, packet_id: str, size: int) -> dict:
        self.queues.setdefault(flow, []).append((packet_id, size))
        self.weights.setdefault(flow, 1)
        self.deficits.setdefault(flow, 0)
        self.served.setdefault(flow, 0)
        self.wal.append(("enqueue", flow, packet_id, size))
        return {"depth": len(self.queues[flow])}

    def dequeue(self, rounds: int = 1) -> dict:
        """基线：按到达顺序出队，不看权重。"""
        sent = []
        for _ in range(rounds):
            for flow in list(self.queues):
                if self.queues[flow]:
                    packet = self.queues[flow].pop(0)
                    sent.append(packet[0])
                    self.served[flow] += packet[1]
                    self.order.append(packet[0])
        return {"sent": sent}

    def set_weight(self, flow: str, weight: int) -> dict:
        self.weights[flow] = weight
        return {"weight": weight}

    def recover(self) -> dict:
        raise NotImplementedError("重启恢复还没实现")

    def stats(self) -> dict:
        return {"queues": {flow: len(items) for flow, items in self.queues.items()},
                "weights": dict(self.weights), "deficits": dict(self.deficits),
                "served": dict(self.served), "order": list(self.order)}
