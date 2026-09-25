import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from drrqueue import DeficitQueue
from server import serve

class TestDeficitQueue(unittest.TestCase):
    def test_enqueue_depth(self):
        queue = DeficitQueue()
        self.assertEqual(queue.enqueue("f1", "p1", 4)["depth"], 1)

    def test_enqueue_then_dequeue(self):
        queue = DeficitQueue()
        queue.enqueue("f1", "p1", 4)
        self.assertEqual(queue.dequeue(1)["sent"], ["p1"])

    def test_empty_dequeue(self):
        self.assertEqual(DeficitQueue().dequeue(1)["sent"], [])

    def test_stats_shape(self):
        self.assertIn("served", DeficitQueue().stats())

    def test_http_enqueue_dequeue(self):
        server = serve(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = "http://127.0.0.1:%d" % server.server_port
        urllib.request.urlopen(base + "/enqueue", data=b'{"flow": "f1", "id": "p1", "size": 4}', timeout=5).read()
        with urllib.request.urlopen(base + "/dequeue", data=b'{"rounds": 1}', timeout=5) as response:
            self.assertEqual(json.loads(response.read())["sent"], ["p1"])
        server.shutdown()

    def test_deficit_carries_across_rounds(self):
        queue = DeficitQueue()
        queue.enqueue("f1", "p1", 4)
        queue.enqueue("f1", "p2", 2)
        queue.set_weight("f1", 2)
        self.assertEqual(queue.dequeue(1)["sent"], [])
        self.assertEqual(queue.dequeue(1)["sent"], ["p1"])
        self.assertEqual(queue.dequeue(1)["sent"], ["p2"])
        self.assertEqual(queue.stats()["deficits"], {"f1": 0})

    def test_weight_change_applies_next_round(self):
        queue = DeficitQueue()
        queue.enqueue("f1", "p1", 3)
        queue.enqueue("f1", "p2", 3)
        queue.set_weight("f1", 2)
        self.assertEqual(queue.dequeue(1)["sent"], [])
        queue.set_weight("f1", 3)
        self.assertEqual(queue.dequeue(1)["sent"], ["p1"])
        self.assertEqual(queue.stats()["deficits"], {"f1": 2})

    def test_recover_restores_state_and_skips_partial_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.wal")
            queue = DeficitQueue(wal_path=path)
            queue.enqueue("f1", "p1", 4)
            queue.enqueue("f1", "p2", 2)
            queue.enqueue("f2", "p3", 3)
            queue.set_weight("f1", 2)
            queue.set_weight("f2", 3)
            queue.dequeue(2)
            with open(path, "ab") as handle:
                handle.write(b'{"op": "enqueue", "flow": "f9", "id": "px"')  # 半条记录
            recovered = DeficitQueue(wal_path=path)
            result = recovered.recover()
            self.assertEqual(result["deficits"], queue.stats()["deficits"])
            self.assertEqual(result["weights"], {"f1": 2, "f2": 3})
            self.assertEqual(result["queues"], {"f1": 1, "f2": 0})
            self.assertNotIn("f9", recovered.stats()["queues"])
