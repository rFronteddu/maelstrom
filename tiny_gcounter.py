import threading
import time

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# CRDT Grow-Only Counter
# ============================================================

# A distributed counter where every node keeps its own private
# increment tally, everyone gossips those tallies, replicas merge
# by taking the largest tally seen for each node, and the total
# counter is the sum of all tallies.”

class GCounterNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
        gossip_interval: float = 0.25,
    ):
        super().__init__(node_id, node_ids, network)

        self.lock = threading.Lock()
        self.gossip_interval = gossip_interval

        # Each node tracks how many increments every replica has performed.
        self.counts: dict[str, int] = {
            peer: 0
            for peer in node_ids
        }


        # ----------------------------------------------------
        # Client increments this node's counter.
        # ----------------------------------------------------
        @self.handler
        def add(req: Request):
            delta = req.body.get("delta", 1)

            # G-Counters are increment-only.
            if delta < 0:
                return {
                    "type": "error",
                    "code": 400,
                    "text": "G-Counter does not support negative increments",
                }
            with self.lock:
                self.counts[self.node_id] += delta
                current_value = sum(self.counts.values())

            print(
                f"[{self.node_id}] local add {delta} -> "
                f"local state={self.counts}, value={current_value}"
            )

            return {
                "type": "add_ok",
                "value": current_value,
            }


        # ----------------------------------------------------
        # Client reads current counter value.
        # ----------------------------------------------------

        @self.handler
        def read(req: Request):
            with self.lock:
                value = sum(self.counts.values())
                snapshot = dict(self.counts)

            print(
                f"[{self.node_id}] read -> "
                f"value={value}, state={snapshot}"
            )

            return {
                "type": "read_ok",
                "value": value,
            }

        # ----------------------------------------------------
        # Peer sends its current CRDT state.
        # ----------------------------------------------------

        @self.handler
        def gossip(req: Request):
            incoming_counts: dict[str, int] = req.body["counts"]

            changed = False

            with self.lock:
                for peer, incoming_value in incoming_counts.items():
                    current_value = self.counts.get(peer, 0)

                    # CRDT merge rule:
                    # max per replica
                    merged_value = max(current_value, incoming_value)

                    if merged_value != current_value:
                        self.counts[peer] = merged_value
                        changed = True


                if changed:
                    snapshot = dict(self.counts)
                    value = sum(self.counts.values())
                else:
                    snapshot = None
                    value = None

            if changed:
                print(
                    f"[{self.node_id}] merged gossip -> "
                    f"state={snapshot}, value={value}"
                )

            return None


        # Start periodic anti-entropy gossip.
        self.gossip_thread = threading.Thread(
            target=self._periodic_gossip_loop,
            daemon=True,
        )
        self.gossip_thread.start()

    def _periodic_gossip_loop(self) -> None:
        while True:
            time.sleep(self.gossip_interval)

            with self.lock:
                snapshot = dict(self.counts)

            self.broadcast_to_peers({
                "type": "gossip",
                "counts": snapshot,
            })


# ============================================================
# Demo
# ============================================================

if __name__ == "__main__":
    network = Network()

    node_ids = ["n0", "n1", "n2", "n3", "n4"]

    nodes = [
        GCounterNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    # Concurrent-ish increments on different nodes.
    client.send("n0", {
        "type": "add",
        "delta": 2,
    })

    client.send("n1", {
        "type": "add",
        "delta": 3,
    })

    client.send("n2", {
        "type": "add",
        "delta": 5,
    })

    # Initially, each node may only know its own increment.
    time.sleep(0.2)

    print("\n--- early reads, before full gossip convergence ---")
    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    # Give periodic gossip enough time to converge.
    time.sleep(1.0)

    print("\n--- reads after gossip convergence ---")
    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1.0)