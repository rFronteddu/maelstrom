import threading
import time

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# Gossip / Broadcast algorithm
# ============================================================

class GossipNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
    ):
        super().__init__(node_id, node_ids, network)

        self.seen: set[int] = set()
        self.lock = threading.Lock()

        # ----------------------------------------------------
        # Client asks this node to broadcast a value.
        # ----------------------------------------------------

        @self.handler
        def broadcast(req: Request):
            message = req.body["message"]
            self._learn_and_gossip(message)

            return {
                "type": "broadcast_ok",
            }

        # ----------------------------------------------------
        # Another node tells us about a value.
        # ----------------------------------------------------

        @self.handler
        def gossip(req: Request):
            message = req.body["message"]
            self._learn_and_gossip(message)

            # Internal node-to-node messages do not need replies.
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

    def _learn_and_gossip(self, message: int) -> None:
        """
        Core flooding-gossip logic:

        1. If we have already seen this message, stop.
        2. Otherwise record it.
        3. Forward it to every peer.
        """
        with self.lock:
            if message in self.seen:
                return

            self.seen.add(message)

        print(f"[{self.node_id}] learned {message}")

        self.broadcast_to_peers({
            "type": "gossip",
            "message": message,
        })


# ============================================================
# Demo
# ============================================================

if __name__ == "__main__":
    network = Network()

    node_ids = ["n0", "n1", "n2", "n3", "n4"]

    nodes = [
        GossipNode(node_id, node_ids, network)
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