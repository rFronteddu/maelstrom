import threading
import time

from tiny_test_lib import Client, Network, Node, Request

# ============================================================
# Coordinator Counter with Simple Leader Replacement
# ============================================================

class FailoverCoordinatorCounterNode(Node):
    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
        heartbeat_interval: float = 0.25,
        leader_timeout: float = 0.8,
    ):
        super().__init__(node_id, node_ids, network)

        self.lock = threading.Lock()

        # Heartbeat configuration.
        self.heartbeat_interval = heartbeat_interval
        self.leader_timeout = leader_timeout

        # Cluster membership knowledge.
        # everyone starts by believing all nodes are alive.
        self.alive_nodes: set[str] = set(node_ids)

        # Initial deterministic leader.
        self.current_leader = min(self.alive_nodes)

        # Followers track when they last heard from the current leader.
        self.last_heartbeat_time = time.time()

        # Only the current leader should serve this counter.
        # Note: this toy example does NOT replicate the counter state,
        # so if the leader dies, the new leader starts from its own local state.
        self.value = 0

        self.heartbeat_thread: threading.Thread | None = None
        self.failure_detector_thread: threading.Thread | None = None

        print(
            f"[{self.node_id}] starting; "
            f"leader={self.current_leader}; "
            f"is_leader={self.is_leader()}"
        )

        # ----------------------------------------------------
        # Client asks to add to the counter.
        # ----------------------------------------------------
        @self.handler
        def add(req: Request):
            delta = req.body.get("delta", 1)

            with self.lock:
                leader = self.current_leader
                am_leader = self.node_id == leader

            if am_leader:
                return self._leader_handle_add(delta)

            print(
                f"[{self.node_id}] forwarding add({delta}) "
                f"from {req.src} to leader {leader}"
            )

            self.send(leader, {
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
            with self.lock:
                leader = self.current_leader
                am_leader = self.node_id == leader

            if am_leader:
                return self._leader_handle_read()

            print(
                f"[{self.node_id}] forwarding read "
                f"from {req.src} to leader {leader}"
            )

            self.send(leader, {
                "type": "forward_read",
                "client_src": req.src,
            })

            return None

        # ----------------------------------------------------
        # Leader receives forwarded add request.
        # ----------------------------------------------------

        @self.handler
        def forward_add(req: Request):
            with self.lock:
                am_leader = self.node_id == self.current_leader

            if not am_leader:
                print(
                    f"[{self.node_id}] received forward_add "
                    f"but I am not the leader"
                )
                return None

            delta = req.body["delta"]
            client_src = req.body["client_src"]

            response = self._leader_handle_add(delta)

            # Reply directly to original client, not to the follower.
            self.send(client_src, response)

            return None

        # ----------------------------------------------------
        # Leader receives forwarded read request.
        # ----------------------------------------------------

        @self.handler
        def forward_read(req: Request):
            with self.lock:
                am_leader = self.node_id == self.current_leader

            if not am_leader:
                print(
                    f"[{self.node_id}] received forward_read "
                    f"but I am not the leader"
                )
                return None


            client_src = req.body["client_src"]

            response = self._leader_handle_read()

            self.send(client_src, response)

            return None


        # ----------------------------------------------------
        # Leader heartbeat
        # ----------------------------------------------------
        @self.handler
        def heartbeat(req: Request):
            leader_id = req.body["leader"]

            with self.lock:
                # If this heartbeat is from the leader we currently believe in,
                # refresh the timeout.
                if leader_id == self.current_leader:
                    self.last_heartbeat_time = time.time()

            return None

        # ----------------------------------------------------
        # Another node announces that it timed out the leader
        # ----------------------------------------------------
        @self.handler
        def leader_failed(req: Request):
            failed_leader = req.body["failed_leader"]

            self._mark_failed_and_recompute_leader(failed_leader)
            return None


    # ----------------------------------------------------
    # Background threads:
    # - leader sends heartbeats
    # - followers monitor for leader timeout
    # ----------------------------------------------------

    def run(self) -> None:
        # First start the normal node event loop.
        super().run()

        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
        )
        self.heartbeat_thread.start()

        self.failure_detector_thread = threading.Thread(
            target=self._failure_detector_loop,
            daemon=True,
        )
        self.failure_detector_thread.start()

    # ========================================================
    # Helpers
    # ========================================================

    def is_leader(self) -> bool:
        with self.lock:
            return self.node_id == self.current_leader


    # --------------------------------------------------------
    # Leader-only business logic
    # --------------------------------------------------------
    def _leader_handle_add(self, delta: int) -> dict:
        with self.lock:
            self.value += delta
            current = self.value
            leader = self.current_leader

        print(
            f"[{self.node_id}] leader {leader} applied add({delta}) "
            f"-> {current}"
        )

        return {
            "type": "add_ok",
            "value": current,
            "leader": leader,
        }

    def _leader_handle_read(self) -> dict:
        with self.lock:
            current = self.value
            leader = self.current_leader

        print(
            f"[{self.node_id}] leader {leader} read -> {current}"
        )

        return {
            "type": "read_ok",
            "value": current,
            "leader": leader,
        }


    # --------------------------------------------------------
    # Leader heartbeat loop
    # --------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        while self.running:
            time.sleep(self.heartbeat_interval)

            with self.lock:
                am_leader = self.node_id == self.current_leader
                leader = self.current_leader

            if not am_leader:
                continue

            print(f"[{self.node_id}] sending heartbeats as leader")

            with self.lock:
                peers = list(self.alive_nodes)

            for peer in peers:
                if peer == self.node_id:
                    continue

                self.send(peer, {
                    "type": "heartbeat",
                    "leader": leader,
                })



    # --------------------------------------------------------
    # Follower failure detection loop
    # --------------------------------------------------------

    def _failure_detector_loop(self) -> None:
        while self.running:
            time.sleep(0.1)

            with self.lock:
                leader = self.current_leader
                am_leader = self.node_id == leader
                elapsed = time.time() - self.last_heartbeat_time

            # The leader does not timeout itself.
            if am_leader:
                continue

            # If leader heartbeat has not been observed for too long,
            # suspect the leader failed.
            if elapsed > self.leader_timeout:
                print(
                    f"[{self.node_id}] timed out leader {leader} "
                    f"after {elapsed:.2f}s"
                )

                self._mark_failed_and_recompute_leader(leader)

                # Tell peers what we believe happened.
                with self.lock:
                    peers = list(self.alive_nodes)

                for peer in peers:
                    if peer == self.node_id:
                        continue

                    self.send(peer, {
                        "type": "leader_failed",
                        "failed_leader": leader,
                    })

    # --------------------------------------------------------
    # Membership update + deterministic replacement
    # --------------------------------------------------------

    def _mark_failed_and_recompute_leader(self, failed_leader: str) -> None:
        with self.lock:
            if failed_leader not in self.alive_nodes:
                return

            self.alive_nodes.discard(failed_leader)

            if not self.alive_nodes:
                print(f"[{self.node_id}] no live nodes left")
                return

            old_leader = self.current_leader
            self.current_leader = min(self.alive_nodes)

            # Reset timeout tracking for the newly chosen leader.
            self.last_heartbeat_time = time.time()

            new_leader = self.current_leader

        print(
            f"[{self.node_id}] leader changed "
            f"{old_leader} -> {new_leader}; "
            f"alive={sorted(self.alive_nodes)}"
        )


if __name__ == "__main__":
    network = Network()

    node_ids = ["n0", "n1", "n2", "n3"]

    nodes = {
        node_id: FailoverCoordinatorCounterNode(
            node_id=node_id,
            node_ids=node_ids,
            network=network,
        )
        for node_id in node_ids
    }

    for node in nodes.values():
        node.run()

    client = Client(network)

    # --------------------------------------------------------
    # Phase 1: n0 is the leader
    # --------------------------------------------------------

    time.sleep(0.5)

    print("\n--- BEFORE LEADER FAILURE ---\n")

    client.send("n2", {
        "type": "add",
        "delta": 5,
    })

    client.send("n3", {
        "type": "add",
        "delta": 7,
    })

    time.sleep(0.5)

    client.send("n1", {
        "type": "read",
    })

    time.sleep(0.8)

    # --------------------------------------------------------
    # Phase 2: crash n0
    # --------------------------------------------------------

    nodes["n0"].crash()

    # Give followers enough time to miss heartbeats,
    # timeout n0, and elect n1.
    time.sleep(1.5)

    # --------------------------------------------------------
    # Phase 3: requests now route to n1
    # --------------------------------------------------------

    print("\n--- AFTER LEADER FAILURE ---\n")

    client.send("n2", {
        "type": "add",
        "delta": 10,
    })

    client.send("n3", {
        "type": "read",
    })

    client.send("n1", {
        "type": "read",
    })

    time.sleep(2)