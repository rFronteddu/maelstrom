import threading
import time
import m_uuid
from typing import Any

from tiny_test_lib import Client, Network, Node, Request


Version = tuple[int, str]
StoredValue = tuple[Version, Any]

# ============================================================
# Quorum-replicated key-value store
#
# N replicas
# quorum = floor(N / 2) + 1
#
# Write:
#   1. Query replicas for current max version of the key
#   2. Choose a strictly newer version
#   3. Write to all replicas
#   4. Return success after quorum ACKs
#
# Read:
#   1. Query replicas
#   2. Wait for quorum responses
#   3. Return value with highest version
# ============================================================

class QuorumKVNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
    ):
        super().__init__(node_id, node_ids, network)

        self.lock = threading.Lock()

        self.quorum_size = len(node_ids) // 2 + 1

        # Local replicated storage:
        #
        #   key -> ((version_number, writer_node_id), value)
        #
        # Example:
        #   "x" -> ((3, "n2"), 100)
        #
        self.store: dict[str, StoredValue] = {}

        # In-flight client operations coordinated by this node.
        #
        # op_id -> metadata
        #
        self.pending: dict[str, dict[str, Any]] = {}

        print(
            f"[{self.node_id}] started; "
            f"cluster_size={len(node_ids)}, quorum={self.quorum_size}"
        )

        # ----------------------------------------------------
        # Client request: write(key, value)
        # ----------------------------------------------------
        @self.handler
        def write(req: Request):
            key = req.body["key"]
            value = req.body["value"]

            op_id = self._new_op_id()

            with self.lock:
                self.pending[op_id] = {
                    "kind": "write",
                    "phase": "query",
                    "client_src": req.src,
                    "key": key,
                    "value": value,
                    "responses": {},
                    "acks": set(),
                }

            print(
                f"[{self.node_id}] starting WRITE op={op_id} "
                f"{key}={value!r}"
            )

            # Query every replica for its current version/value of this key.
            for peer in self.node_ids:
                self.send(peer, {
                    "type": "query_replica",
                    "op_id": op_id,
                    "coordinator": self.node_id,
                    "key": key,
                })

            return None

        # ----------------------------------------------------
        # Client request: read(key)
        # ----------------------------------------------------
        @self.handler
        def read(req: Request):
            key = req.body["key"]

            op_id = self._new_op_id()

            with self.lock:
                self.pending[op_id] = {
                    "kind": "read",
                    "phase": "query",
                    "client_src": req.src,
                    "key": key,
                    "responses": {},
                }

            print(
                f"[{self.node_id}] starting READ op={op_id} "
                f"key={key!r}"
            )

            # Query every replica.
            for peer in self.node_ids:
                self.send(peer, {
                    "type": "query_replica",
                    "op_id": op_id,
                    "coordinator": self.node_id,
                    "key": key,
                })

            return None

        # ----------------------------------------------------
        # Replica handler:
        # Coordinator asks:
        #   "What version/value do you have for this key?"
        # ----------------------------------------------------
        @self.handler
        def query_replica(req: Request):
            op_id = req.body["op_id"]
            coordinator = req.body["coordinator"]
            key = req.body["key"]

            with self.lock:
                version, value = self.store.get(
                    key,
                    ((0, ""), None),
                )

            print(
                f"[{self.node_id}] query for {key!r} -> "
                f"version={version}, value={value!r}"
            )

            self.send(coordinator, {
                "type": "replica_response",
                "op_id": op_id,
                "replica": self.node_id,
                "key": key,
                "version": version,
                "value": value,
            })

            return None

        # ----------------------------------------------------
        # Coordinator handler:
        # Receives a query response from a replica.
        # This can belong to either:
        #   - a read operation
        #   - the first phase of a write operation
        # ----------------------------------------------------
        @self.handler
        def replica_response(req: Request):
            op_id = req.body["op_id"]
            replica = req.body["replica"]
            version: Version = tuple(req.body["version"])
            value = req.body["value"]

            messages_to_send: list[tuple[str, dict[str, Any]]] = []
            client_reply: tuple[str, dict[str, Any]] | None = None

            with self.lock:
                op = self.pending.get(op_id)

                if op is None:
                    return None

                if op["phase"] != "query":
                    return None

                # Ignore duplicate response from same replica.
                if replica in op["responses"]:
                    return None

                op["responses"][replica] = {
                    "version": version,
                    "value": value,
                }

                response_count = len(op["responses"])

                if response_count < self.quorum_size:
                    return None

                # We now have a quorum of responses.
                highest_version, highest_value = self._highest_response(
                    op["responses"]
                )

                # ------------------------------------------------
                # READ: return highest-version value in the quorum.
                # ------------------------------------------------
                if op["kind"] == "read":
                    print(
                        f"[{self.node_id}] READ op={op_id} "
                        f"resolved version={highest_version}, "
                        f"value={highest_value!r}"
                    )

                    client_reply = (
                        op["client_src"],
                        {
                            "type": "read_ok",
                            "key": op["key"],
                            "value": highest_value,
                            "version": highest_version,
                        },
                    )

                    del self.pending[op_id]

                # ------------------------------------------------
                # WRITE:
                # Choose a new version greater than anything seen
                # in the quorum, then begin replication phase.
                # ------------------------------------------------
                elif op["kind"] == "write":
                    next_version: Version = (
                        highest_version[0] + 1,
                        self.node_id,
                    )

                    op["phase"] = "write"
                    op["version"] = next_version

                    key = op["key"]
                    new_value = op["value"]

                    print(
                        f"[{self.node_id}] WRITE op={op_id} "
                        f"picked version={next_version} "
                        f"for {key}={new_value!r}"
                    )

                    for peer in self.node_ids:
                        messages_to_send.append((
                            peer,
                            {
                                "type": "store_replica",
                                "op_id": op_id,
                                "coordinator": self.node_id,
                                "key": key,
                                "version": next_version,
                                "value": new_value,
                            },
                        ))

            # Send outside lock.
            if client_reply is not None:
                dest, body = client_reply
                self.send(dest, body)

            for dest, body in messages_to_send:
                self.send(dest, body)

            return None

        # ----------------------------------------------------
        # Replica handler:
        # Coordinator asks replica to store version/value.
        # ----------------------------------------------------
        @self.handler
        def store_replica(req: Request):
            op_id = req.body["op_id"]
            coordinator = req.body["coordinator"]
            key = req.body["key"]
            incoming_version: Version = tuple(req.body["version"])
            incoming_value = req.body["value"]

            with self.lock:
                current_version, current_value = self.store.get(
                    key,
                    ((0, ""), None),
                )

                # Only move forward to newer versions.
                if incoming_version > current_version:
                    self.store[key] = (
                        incoming_version,
                        incoming_value,
                    )

                    print(
                        f"[{self.node_id}] STORED {key}={incoming_value!r} "
                        f"at version={incoming_version}"
                    )
                else:
                    print(
                        f"[{self.node_id}] IGNORE older/equal write "
                        f"for {key}: incoming={incoming_version}, "
                        f"current={current_version}"
                    )

            # ACK whether we stored it or already had a newer/equal value.
            # For quorum progress, "I am at least this up-to-date" is enough.
            self.send(coordinator, {
                "type": "store_ack",
                "op_id": op_id,
                "replica": self.node_id,
            })

            return None

        # ----------------------------------------------------
        # Coordinator handler:
        # Receives acknowledgments for the second phase of a write.
        # Once quorum ACKs arrive, the write succeeds.
        # ----------------------------------------------------
        @self.handler
        def store_ack(req: Request):
            op_id = req.body["op_id"]
            replica = req.body["replica"]

            client_reply: tuple[str, dict[str, Any]] | None = None

            with self.lock:
                op = self.pending.get(op_id)

                if op is None:
                    return None

                if op["kind"] != "write":
                    return None

                if op["phase"] != "write":
                    return None

                op["acks"].add(replica)

                ack_count = len(op["acks"])

                print(
                    f"[{self.node_id}] WRITE op={op_id} "
                    f"acks={ack_count}/{self.quorum_size}"
                )

                if ack_count < self.quorum_size:
                    return None

                key = op["key"]
                value = op["value"]
                version = op["version"]

                print(
                    f"[{self.node_id}] WRITE op={op_id} COMMITTED "
                    f"{key}={value!r}, version={version}"
                )

                client_reply = (
                    op["client_src"],
                    {
                        "type": "write_ok",
                        "key": key,
                        "value": value,
                        "version": version,
                    },
                )

                del self.pending[op_id]

            if client_reply is not None:
                dest, body = client_reply
                self.send(dest, body)

            return None

    # ========================================================
    # Utility helpers
    # ========================================================
    def _new_op_id(self) -> str:
        return uuid.uuid4().hex[:8]


    def _highest_response(
        self,
        responses: dict[str, dict[str, Any]],
    ) -> StoredValue:
        highest_version: Version = (0, "")
        highest_value: Any = None

        for response in responses.values():
            version: Version = response["version"]
            value = response["value"]

            if version > highest_version:
                highest_version = version
                highest_value = value

        return highest_version, highest_value


if __name__ == "__main__":
    network = Network(
        min_delay=0.02,
        max_delay=0.12,
    )

    node_ids = ["n0", "n1", "n2", "n3", "n4"]

    nodes = [
        QuorumKVNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    time.sleep(0.3)

    print("\n--- WRITE x=100 through n4 ---\n")

    client.send("n4", {
        "type": "write",
        "key": "x",
        "value": 100,
    })

    time.sleep(1.5)

    print("\n--- READ x through n2 ---\n")

    client.send("n2", {
        "type": "read",
        "key": "x",
    })

    time.sleep(1.5)

    print("\n--- WRITE x=200 through n1 ---\n")

    client.send("n1", {
        "type": "write",
        "key": "x",
        "value": 200,
    })

    time.sleep(1.5)

    print("\n--- READ x through n3 ---\n")

    client.send("n3", {
        "type": "read",
        "key": "x",
    })

    time.sleep(2)