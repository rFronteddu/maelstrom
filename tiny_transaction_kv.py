import threading
import time
import uuid
from typing import Any

from tiny_test_lib import Client, Network, Node, Request


# ============================================================
# Transactional Key-Value Store
#
# Design:
# - Fixed coordinator/leader = smallest node ID
# - Clients may send txn requests to any node
# - Followers forward txn requests to leader
# - Leader executes the entire transaction under one lock
# - Writes are buffered until commit
# - Reads see earlier writes from the same transaction
# - After commit, writes are asynchronously replicated to followers
#
# This is atomic at the leader, but not a full distributed transaction
# protocol. Replication happens after the transaction commits.
# ============================================================

class TransactionKVNode(Node):
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

        # Local committed key-value state.
        self.store: dict[Any, Any] = {}

        # Optional: track committed transaction IDs.
        self.committed_txns: dict[str, dict[Any, Any]] = {}

        print(
            f"[{self.node_id}] leader={self.leader_id}, "
            f"is_leader={self.is_leader}"
        )

        # ----------------------------------------------------
        # Client-facing transaction request
        # ----------------------------------------------------

        @self.handler
        def txn(req: Request):
            req_txn = req.body["txn"]

            if self.is_leader:
                return self._leader_execute_txn(req_txn)

            print(
                f"[{self.node_id}] forwarding txn from {req.src} "
                f"to leader {self.leader_id}: {req_txn}"
            )

            self.send(self.leader_id, {
                "type": "forward_txn",
                "txn": req_txn,
                "client_src": req.src,
            })

            # Leader replies directly to original client.
            return None

        # ----------------------------------------------------
        # Leader receives forwarded transaction
        # ----------------------------------------------------

        @self.handler
        def forward_txn(req: Request):
            if not self.is_leader:
                print(
                    f"[{self.node_id}] ERROR: non-leader received forward_txn"
                )
                return None

            req_txn = req.body["txn"]
            client_src = req.body["client_src"]

            response = self._leader_execute_txn(req_txn)

            self.send(client_src, response)
            return None

        # ----------------------------------------------------
        # Followers receive committed writes after leader commit
        # ----------------------------------------------------

        @self.handler
        def replicate_txn(req: Request):
            tx_id = req.body["tx_id"]
            writes = req.body["writes"]

            with self.lock:
                for key, value in writes.items():
                    self.store[key] = value

                self.committed_txns[tx_id] = dict(writes)
                snapshot = dict(self.store)

            print(
                f"[{self.node_id}] replicated tx={tx_id}, "
                f"writes={writes}, store={snapshot}"
            )

            return None

        # ----------------------------------------------------
        # Debug helper: local read outside a transaction
        # Useful to observe replication state on followers.
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

    # ========================================================
    # Leader transaction logic
    # ========================================================

    def _leader_execute_txn(self, req_txn: list[list[Any]]) -> dict:
        """
        Execute one whole transaction atomically at the leader.

        req_txn example:
            [
                ["r", "x", None],
                ["w", "x", 10],
                ["r", "x", None],
            ]
        """

        tx_id = uuid.uuid4().hex[:8]

        # Make a copy because we will mutate read operations
        # by filling in their returned value.
        result_txn = [
            list(op)
            for op in req_txn
        ]

        writes: dict[Any, Any] = {}

        with self.lock:
            print(
                f"[{self.node_id}] BEGIN tx={tx_id}, txn={result_txn}"
            )

            for op in result_txn:
                kind = op[0]
                key = op[1]

                # --------------------------------------------
                # Read operation
                # --------------------------------------------
                if kind == "r":
                    # Read your own writes first.
                    if key in writes:
                        op[2] = writes[key]
                    else:
                        op[2] = self.store.get(key)

                # --------------------------------------------
                # Write operation
                # --------------------------------------------
                elif kind == "w":
                    value = op[2]

                    # Buffer write. Do not publish it yet.
                    writes[key] = value

                else:
                    raise ValueError(f"Unknown transaction op: {kind!r}")

            # --------------------------------------------
            # Commit atomically:
            # apply all buffered writes to committed state.
            # --------------------------------------------
            for key, value in writes.items():
                self.store[key] = value

            self.committed_txns[tx_id] = dict(writes)

            snapshot = dict(self.store)

            print(
                f"[{self.node_id}] COMMIT tx={tx_id}, "
                f"writes={writes}, store={snapshot}"
            )

        # ------------------------------------------------
        # Replicate after commit.
        # This happens outside the lock.
        # ------------------------------------------------
        if writes:
            for peer in self.node_ids:
                if peer == self.node_id:
                    continue

                self.send(peer, {
                    "type": "replicate_txn",
                    "tx_id": tx_id,
                    "writes": dict(writes),
                })

        return {
            "type": "txn_ok",
            "txn": result_txn,
            "tx_id": tx_id,
        }


# ============================================================
# Demo
# ============================================================

if __name__ == "__main__":
    network = Network(
        min_delay=0.02,
        max_delay=0.12,
    )

    node_ids = ["n0", "n1", "n2", "n3"]

    nodes = [
        TransactionKVNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    time.sleep(0.3)

    print("\n--- TXN 1 sent to follower n2 ---\n")

    client.send("n2", {
        "type": "txn",
        "txn": [
            ["r", "x", None],   # x does not exist yet -> None
            ["w", "x", 10],     # buffer x=10
            ["r", "x", None],   # read own write -> 10
            ["w", "y", 20],     # buffer y=20
        ],
    })

    time.sleep(1.0)

    print("\n--- TXN 2 sent to follower n3 ---\n")

    client.send("n3", {
        "type": "txn",
        "txn": [
            ["r", "x", None],   # should see committed 10
            ["r", "y", None],   # should see committed 20
            ["w", "x", 99],     # update x
            ["r", "x", None],   # read own write -> 99
        ],
    })

    time.sleep(1.0)

    print("\n--- LOCAL READS AFTER REPLICATION ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read_local",
            "key": "x",
        })

    time.sleep(1.5)