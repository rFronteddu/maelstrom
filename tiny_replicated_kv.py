import threading
import time
from typing import Any

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# Replicated Key-Value Store
#
# Design:
# - Fixed leader = smallest node ID
# - Writes sent to followers are forwarded to leader
# - Leader applies writes locally
# - Leader asynchronously replicates writes to followers
# - Local reads may be stale
# - Leader reads give the latest value known to the leader
# ============================================================

class ReplicatedKVNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
    ):
        super().__init__(node_id, node_ids, network)

        self.lock = threading.Lock()

        self.leader_id = min(node_ids)
        self.is_leader = self.node_id == self.leader_id

        # Local copy of the key-value store.
        self.store: dict[str, Any] = {}

        print(
            f"[{self.node_id}] leader={self.leader_id}, "
            f"is_leader={self.is_leader}"
        )

        # ----------------------------------------------------
        # Client request: write key=value
        # ----------------------------------------------------

        @self.handler
        def write(req: Request):
            key = req.body["key"]
            value = req.body["value"]

            if self.is_leader:
                return self._leader_write(key, value)

            print(
                f"[{self.node_id}] forwarding write "
                f"{key}={value!r} from {req.src} to leader {self.leader_id}"
            )

            self.send(self.leader_id, {
                "type": "forward_write",
                "key": key,
                "value": value,
                "client_src": req.src,
            })

            # Leader will reply directly to the client.
            return None

        # ----------------------------------------------------
        # Leader receives a forwarded write
        # ----------------------------------------------------
        @self.handler
        def forward_write(req: Request):
            if not self.is_leader:
                print(
                    f"[{self.node_id}] ERROR: non-leader received forward_write"
                )
                return None

            key = req.body["key"]
            value = req.body["value"]
            client_src = req.body["client_src"]

            response = self._leader_write(key, value)

            # Reply directly to the original client.
            self.send(client_src, response)

            return None

        # ----------------------------------------------------
        # Replication message: leader tells follower to apply write
        # ----------------------------------------------------
        @self.handler
        def replicate_write(req: Request):
            key = req.body["key"]
            value = req.body["value"]

            with self.lock:
                self.store[key] = value
                snapshot = dict(self.store)

            print(
                f"[{self.node_id}] replicated {key}={value!r}; "
                f"store={snapshot}"
            )

            return None

        # ----------------------------------------------------
        # Client request: local read
        #
        # Fast, but may be stale if replication has not arrived yet.
        # ----------------------------------------------------
        @self.handler
        def read_local(req: Request):
            key = req.body["key"]

            with self.lock:
                value = self.store.get(key)

            print(
                f"[{self.node_id}] local read {key!r} -> {value!r}"
            )

            return {
                "type": "read_local_ok",
                "key": key,
                "value": value,
                "node": self.node_id,
            }

        # ----------------------------------------------------
        # Client request: read from leader
        #
        # If sent to a follower, follower forwards to leader.
        # ----------------------------------------------------

        @self.handler
        def read_leader(req: Request):
            key = req.body["key"]

            if self.is_leader:
                return self._leader_read(key)

            print(
                f"[{self.node_id}] forwarding leader-read "
                f"{key!r} from {req.src} to leader {self.leader_id}"
            )

            self.send(self.leader_id, {
                "type": "forward_read_leader",
                "key": key,
                "client_src": req.src,
            })

            return None

        # ----------------------------------------------------
        # Leader receives forwarded leader-read
        # ----------------------------------------------------

        @self.handler
        def forward_read_leader(req: Request):
            if not self.is_leader:
                print(
                    f"[{self.node_id}] ERROR: non-leader received forward_read_leader"
                )
                return None

            key = req.body["key"]
            client_src = req.body["client_src"]

            response = self._leader_read(key)
            self.send(client_src, response)

            return None

    # ========================================================
    # Leader-only helpers
    # ========================================================
    def _leader_write(self, key: str, value: Any) -> dict:
        """
        Leader:
        1. Applies write locally.
        2. Asynchronously sends replication to followers.
        3. Immediately replies to client.

        This is intentionally simple and NOT durable if the leader dies
        before followers receive the replication message.
        """
        with self.lock:
            self.store[key] = value
            snapshot = dict(self.store)

        print(
            f"[{self.node_id}] leader wrote {key}={value!r}; "
            f"store={snapshot}"
        )

        # Asynchronous fan-out replication.
        for peer in self.node_ids:
            if peer == self.node_id:
                continue

            self.send(peer, {
                "type": "replicate_write",
                "key": key,
                "value": value,
            })

        return {
            "type": "write_ok",
            "key": key,
            "value": value,
            "leader": self.node_id,
        }

    def _leader_read(self, key: str) -> dict:
        with self.lock:
            value = self.store.get(key)

        print(
            f"[{self.node_id}] leader read {key!r} -> {value!r}"
        )

        return {
            "type": "read_leader_ok",
            "key": key,
            "value": value,
            "leader": self.node_id,
        }

if __name__ == "__main__":
    # Increase delay slightly so stale local reads are easier to observe.
    network = Network(
        min_delay=0.02,
        max_delay=0.15,
    )

    node_ids = ["n0", "n1", "n2", "n3"]

    nodes = [
        ReplicatedKVNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    time.sleep(0.2)

    print("\n--- WRITE SENT TO FOLLOWER n2 ---\n")

    client.send("n2", {
        "type": "write",
        "key": "x",
        "value": 100,
    })

    # Very soon after the write, ask followers for local state.
    # Depending on timing, they may not have replication yet.
    time.sleep(0.05)

    print("\n--- EARLY LOCAL READS: MAY BE STALE ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read_local",
            "key": "x",
        })

    time.sleep(0.7)

    print("\n--- LATER LOCAL READS: SHOULD HAVE CONVERGED ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read_local",
            "key": "x",
        })

    time.sleep(0.5)

    print("\n--- LEADER READ FROM A FOLLOWER ---\n")

    client.send("n3", {
        "type": "read_leader",
        "key": "x",
    })

    time.sleep(1)