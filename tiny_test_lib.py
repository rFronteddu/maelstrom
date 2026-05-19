from __future__ import annotations

import queue
import random
import threading
import time

from dataclasses import dataclass
from typing import Any, Callable


# ============================================================
# Request / Message
# ============================================================

@dataclass
class Request:
    src: str
    dest: str
    body: dict[str, Any]


# ============================================================
# In-memory network
# ============================================================

class Network:
    """
    Tiny in-process distributed system simulator.

    - Nodes register with the network.
    - Messages are delivered into the destination node's inbox.
    - Optional random delay simulates asynchronous delivery.
    - Nodes can be unregistered to simulate crashes.
    """

    def __init__(
        self,
        min_delay: float = 0.001,
        max_delay: float = 0.01,
    ):
        self.nodes: dict[str, Node] = {}
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.lock = threading.Lock()

    def register(self, node: Node) -> None:
        with self.lock:
            self.nodes[node.node_id] = node

    def unregister(self, node_id: str) -> None:
        with self.lock:
            self.nodes.pop(node_id, None)

    def send(self, src: str, dest: str, body: dict[str, Any]) -> None:
        """
        Deliver a message asynchronously.

        We spawn a tiny delivery thread so that sending does not block
        the node's event loop. This better resembles network behavior.
        """
        delivery_thread = threading.Thread(
            target=self._deliver,
            args=(src, dest, body),
            daemon=True,
        )
        delivery_thread.start()

    def _deliver(self, src: str, dest: str, body: dict[str, Any]) -> None:
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

        with self.lock:
            node = self.nodes.get(dest)

        # In a crash/failure simulation, messages to dead nodes are dropped.
        if node is None:
            print(f"[network] unknown destination: {dest}")
            return

        req = Request(
            src=src,
            dest=dest,
            body=body,
        )

        node.inbox.put(req)


# ============================================================
# Base Node
# ============================================================

Handler = Callable[[Request], dict[str, Any] | None]


class Node:
    """
    Base class for all algorithm examples.

    Example:

        class EchoNode(Node):
            def __init__(...):
                super().__init__(...)

                @self.handler
                def echo(req):
                    return {
                        "type": "echo_ok",
                        "echo": req.body["echo"],
                    }
    """

    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
    ):
        self.node_id = node_id
        self.node_ids = node_ids
        self.network = network

        self.inbox: queue.Queue[Request] = queue.Queue()
        self.handlers: dict[str, Handler] = {}

        self.running = False
        self.thread: threading.Thread | None = None

        self.network.register(self)

    # --------------------------------------------------------
    # Handler registration
    # --------------------------------------------------------

    def handler(self, fn: Handler) -> Handler:
        """
        Decorator:

            @self.handler
            def gossip(req):
                ...

        The function name becomes the message type.
        """
        self.handlers[fn.__name__] = fn
        return fn

    # --------------------------------------------------------
    # Messaging helpers
    # --------------------------------------------------------

    def send(self, dest: str, body: dict[str, Any]) -> None:
        self.network.send(
            src=self.node_id,
            dest=dest,
            body=body,
        )

    def broadcast_to_peers(self, body: dict[str, Any]) -> None:
        for peer in self.node_ids:
            if peer != self.node_id:
                self.send(peer, body)

    def reply(self, req: Request, body: dict[str, Any]) -> None:
        self.send(req.src, body)

    # --------------------------------------------------------
    # Runtime
    # --------------------------------------------------------

    def run(self) -> None:
        self.running = True
        self.thread = threading.Thread(
            target=self._event_loop,
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.running = False

    def crash(self) -> None:
        """
        Simulate a node crash:
        - stop its event loop
        - remove it from the network
        - all future messages to it are dropped
        """
        print(f"\n*** [{self.node_id}] CRASHED ***\n")
        self.running = False
        self.network.unregister(self.node_id)


    def _event_loop(self) -> None:
        while self.running:
            req = self.inbox.get()

            msg_type = req.body.get("type")
            if msg_type is None:
                print(f"[{self.node_id}] received message without type: {req.body}")
                continue

            handler = self.handlers.get(msg_type)
            if handler is None:
                print(f"[{self.node_id}] no handler for type={msg_type}")
                continue

            response = handler(req)

            if response is not None:
                self.reply(req, response)


# ============================================================
# Tiny client helper
# ============================================================

class Client:
    """
    Tiny synthetic client that can:
    - send requests into the cluster
    - receive and print replies from nodes
    """

    def __init__(self, network: Network, client_id: str = "client"):
        self.network = network
        self.node_id = client_id
        self.inbox: queue.Queue[Request] = queue.Queue()

        self.network.register(self)

        self.running = True
        self.thread = threading.Thread(
            target=self._event_loop,
            daemon=True,
        )
        self.thread.start()

    def send(self, dest: str, body: dict[str, Any]) -> None:
        self.network.send(
            src=self.node_id,
            dest=dest,
            body=body,
        )

    def _event_loop(self) -> None:
        while self.running:
            req = self.inbox.get()
            print(f"[client] reply from {req.src}: {req.body}")

    def stop(self) -> None:
        self.running = False