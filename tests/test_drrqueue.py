import json
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
