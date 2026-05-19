#!/usr/bin/env python3
from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any

from tiny_test_lib import Client, Network, Node, Request


Tag = tuple[str, int]


class ORSetNode(Node):
    """
    Add-wins Observed-Remove Set (OR-Set) CRDT.

    State:
        adds[element] = all unique add-tags observed for that element
        removes[element] = add-tags that have been removed

    An element is visible iff:
        adds[element] - removes[element]
    is non-empty.
    """

    def __init__(
        self,
        node_id: str,
        node_ids: list[str],
        network: Network,
        gossip_interval: float = 0.25,
    ):
        super().__init__(node_id, node_ids, network)

        self.lock = threading.Lock()

        self.adds: defaultdict[Any, set[Tag]] = defaultdict(set)
        self.removes: defaultdict[Any, set[Tag]] = defaultdict(set)

        # Fresh tag generation: (node_id, local counter)
        self.next_tag_id = 0

        self.gossip_interval = gossip_interval
        self.gossip_thread: threading.Thread | None = None

        @self.handler
        def add(req: Request):
            element = req.body["element"]

            with self.lock:
                self.next_tag_id += 1
                tag: Tag = (self.node_id, self.next_tag_id)
                self.adds[element].add(tag)

                visible = self._visible_elements_locked()

            print(
                f"[{self.node_id}] add({element!r}) "
                f"tag={tag}; visible={visible}"
            )

            return {
                "type": "add_ok",
                "element": element,
            }

        @self.handler
        def remove(req: Request):
            element = req.body["element"]

            with self.lock:
                # Observed-remove rule:
                # only remove add-tags this replica has already seen.
                observed_tags = set(self.adds[element])
                self.removes[element].update(observed_tags)

                visible = self._visible_elements_locked()

            print(
                f"[{self.node_id}] remove({element!r}) "
                f"removed_tags={observed_tags}; visible={visible}"
            )

            return {
                "type": "remove_ok",
                "element": element,
            }

        @self.handler
        def read(req: Request):
            with self.lock:
                elements = self._visible_elements_locked()

            print(f"[{self.node_id}] read -> {elements}")

            return {
                "type": "read_ok",
                "elements": elements,
            }

        @self.handler
        def gossip(req: Request):
            incoming_adds = req.body["adds"]
            incoming_removes = req.body["removes"]

            with self.lock:
                changed = False

                # Merge adds by set union.
                for element, tag_list in incoming_adds.items():
                    incoming_tags = {
                        tuple(tag)
                        for tag in tag_list
                    }

                    before = len(self.adds[element])
                    self.adds[element].update(incoming_tags)

                    if len(self.adds[element]) != before:
                        changed = True

                # Merge removes by set union.
                for element, tag_list in incoming_removes.items():
                    incoming_tags = {
                        tuple(tag)
                        for tag in tag_list
                    }

                    before = len(self.removes[element])
                    self.removes[element].update(incoming_tags)

                    if len(self.removes[element]) != before:
                        changed = True

                visible = self._visible_elements_locked() if changed else None

            if changed:
                print(
                    f"[{self.node_id}] merged gossip from {req.src}; "
                    f"visible={visible}"
                )

            return None

    def run(self) -> None:
        super().run()

        self.gossip_thread = threading.Thread(
            target=self._gossip_loop,
            daemon=True,
        )
        self.gossip_thread.start()

    def _visible_elements_locked(self) -> list[Any]:
        """
        Caller must hold self.lock.
        """
        visible: list[Any] = []

        for element, add_tags in self.adds.items():
            live_tags = add_tags - self.removes[element]

            if live_tags:
                visible.append(element)

        return sorted(visible)

    def _snapshot_state_locked(
        self,
    ) -> tuple[dict[Any, list[Tag]], dict[Any, list[Tag]]]:
        """
        Caller must hold self.lock.
        """
        adds_snapshot = {
            element: list(tags)
            for element, tags in self.adds.items()
        }

        removes_snapshot = {
            element: list(tags)
            for element, tags in self.removes.items()
        }

        return adds_snapshot, removes_snapshot


    def _gossip_loop(self) -> None:
        while True:
            time.sleep(self.gossip_interval)

            with self.lock:
                adds_snapshot, removes_snapshot = self._snapshot_state_locked()

            for peer in self.node_ids:
                if peer == self.node_id:
                    continue

                self.send(peer, {
                    "type": "gossip",
                    "adds": adds_snapshot,
                    "removes": removes_snapshot,
                })

if __name__ == "__main__":
    network = Network(
        min_delay=0.02,
        max_delay=0.12,
    )

    node_ids = ["n0", "n1", "n2", "n3"]

    nodes = [
        ORSetNode(node_id, node_ids, network)
        for node_id in node_ids
    ]

    for node in nodes:
        node.run()

    client = Client(network)

    time.sleep(0.3)

    # --------------------------------------------------------
    # Demo 1:
    # add apple, allow replication, then remove it.
    # Result: apple disappears everywhere.
    # --------------------------------------------------------

    print("\n--- DEMO 1: ADD apple ---\n")

    client.send("n0", {
        "type": "add",
        "element": "apple",
    })

    time.sleep(1.0)

    print("\n--- READ after ADD apple ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1.0)

    print("\n--- REMOVE apple after it has replicated ---\n")

    client.send("n2", {
        "type": "remove",
        "element": "apple",
    })

    time.sleep(1.0)

    print("\n--- READ after REMOVE apple ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1.0)

    # --------------------------------------------------------
    # Demo 2:
    # concurrent add/remove banana.
    # n1 removes banana before observing n0's add-tag.
    # Result: banana remains present: add-wins.
    # --------------------------------------------------------

    print("\n--- DEMO 2: CONCURRENT ADD banana / REMOVE banana ---\n")

    client.send("n0", {
        "type": "add",
        "element": "banana",
    })

    client.send("n1", {
        "type": "remove",
        "element": "banana",
    })

    time.sleep(1.5)

    print("\n--- READ after concurrent ADD/REMOVE banana ---\n")

    for node_id in node_ids:
        client.send(node_id, {
            "type": "read",
        })

    time.sleep(1.5)