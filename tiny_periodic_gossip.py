import threading
import time

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# Gossip / Broadcast algorithm
# ============================================================

class PeriodicGossipNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
        gossip_interval: float = 0.25,
    ):
        super().__init__(node_id, node_ids, network)

        self.seen: set[int] = set()
        self.lock = threading.Lock()
        self.gossip_interval = gossip_interval

        # ----------------------------------------------------
        # Client asks this node to broadcast a value.
        # ----------------------------------------------------

        @self.handler
        def broadcast(req: Request):
            message = req.body["message"]

            with self.lock:
                self.seen.add(message)


            return {
                "type": "broadcast_ok",
            }

        # ----------------------------------------------------
        # Another node tells us about a value.
        # ----------------------------------------------------

        @self.handler
        def gossip(req: Request):
            incoming = req.body["messages"]
            with self.lock:
                before = len(self.seen)
                self.seen.update(incoming)
                after = len(self.seen)

            if after > before:
                print(f"[{self.node_id}] learned new messages: {incoming}")

            return None

        # ----------------------------------------------------
        # Client asks this node what it knows.
        # ----------------------------------------------------

        @self.handler
        def read(req: Request):
            with self.lock:
                messages = sorted(self.seen)

            print(f"[{self.node_id}] read -> {messages}")

            return {
                "type": "read_ok",
                "messages": messages,
            }

        self.gossip_thread = threading.Thread(
            target=self._periodic_gossip_loop,
            daemon=True,
        )
        self.gossip_thread.start()

    def _periodic_gossip_loop(self):
        while True:
            time.sleep(self.gossip_interval)

            with self.lock:
                snapshot = list(self.seen)

            if not snapshot:
                continue

            self.broadcast_to_peers({
                "type": "gossip",
                "messages": snapshot,
            })

# ============================================================
# Demo
# ============================================================

if __name__ == "__main__":
    network = Network()

    node_ids = ["n0", "n1", "n2", "n3", "n4"]

    nodes = [
        PeriodicGossipNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    # Broadcast a few values from different entry points.
    client.send("n0", {
        "type": "broadcast",
        "message": 10,
    })

    client.send("n2", {
        "type": "broadcast",
        "message": 20,
    })

    client.send("n4", {
        "type": "broadcast",
        "message": 30,
    })

    # Give the simulated network time to deliver all gossip.
    time.sleep(1)

    # Read every node.
    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1)