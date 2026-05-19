import threading
import time

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# Fixed-leader coordinator counter
# ============================================================

class CoordinatorCounterNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
    ):
        super().__init__(node_id, node_ids, network)

        # Every node independently computes the same leader.
        self.leader_id = min(node_ids)

        self.is_leader = self.node_id == self.leader_id

        # Only the leader meaningfully uses this counter.
        self.value = 0
        self.lock = threading.Lock()

        print(
            f"[{self.node_id}] leader is {self.leader_id} "
            f"(am I leader? {self.is_leader})"
        )

        # ----------------------------------------------------
        # Client asks to add to the counter.
        # ----------------------------------------------------
        @self.handler
        def add(req: Request):
            delta = req.body.get("delta", 1)

            if self.is_leader:
                # Client happened to contact the leader directly.
                return self._handle_add(delta)

            # Follower forwards to leader.
            print(
                f"[{self.node_id}] forwarding add({delta}) "
                f"from {req.src} to leader {self.leader_id}"
            )

            self.send(self.leader_id, {
                "type": "forward_add",
                "delta": delta,
                "client_src": req.src,
            })

            # Do not reply here.
            # The leader will reply directly to the original client.
            return None


        # ----------------------------------------------------
        # Client asks to read the counter.
        # ----------------------------------------------------

        @self.handler
        def read(req: Request):
            if self.is_leader:
                # Client happened to contact the leader directly.
                return self._handle_read()

            # Follower forwards to leader.
            print(
                f"[{self.node_id}] forwarding read "
                f"from {req.src} to leader {self.leader_id}"
            )

            self.send(self.leader_id, {
                "type": "forward_read",
                "client_src": req.src,
            })

            return None

        # ----------------------------------------------------
        # Leader receives forwarded add request.
        # ----------------------------------------------------

        @self.handler
        def forward_add(req: Request):
            if not self.is_leader:
                print(
                    f"[{self.node_id}] ERROR: non-leader got forward_add"
                )
                return None

            delta = req.body["delta"]
            client_src = req.body["client_src"]

            response = self._handle_add(delta)

            # Reply directly to original client, not to the follower.
            self.send(client_src, response)

            return None

        # ----------------------------------------------------
        # Leader receives forwarded read request.
        # ----------------------------------------------------

        @self.handler
        def forward_read(req: Request):
            if not self.is_leader:
                print(
                    f"[{self.node_id}] ERROR: non-leader got forward_read"
                )
                return None

            client_src = req.body["client_src"]

            response = self._handle_read()

            self.send(client_src, response)

            return None

    # --------------------------------------------------------
    # Leader-only business logic
    # --------------------------------------------------------
    def _handle_add(self, delta: int) -> dict:
        with self.lock:
            self.value += delta
            current = self.value

        print(
            f"[{self.node_id}] leader applied add({delta}) -> {current}"
        )

        return {
            "type": "add_ok",
            "value": current,
        }

    def _handle_read(self) -> dict:
        with self.lock:
            current = self.value

        print(
            f"[{self.node_id}] leader read -> {current}"
        )

        return {
            "type": "read_ok",
            "value": current,
        }


if __name__ == "__main__":
    network = Network()

    node_ids = ["n0", "n1", "n2", "n3"]

    nodes = [
        CoordinatorCounterNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    # Send adds to different nodes.
    # All of them eventually route to n0.
    client.send("n2", {
        "type": "add",
        "delta": 5,
    })

    client.send("n3", {
        "type": "add",
        "delta": 7,
    })

    client.send("n0", {
        "type": "add",
        "delta": 3,
    })

    time.sleep(0.5)

    # Read from multiple nodes.
    # Followers forward the read to the leader.
    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1)