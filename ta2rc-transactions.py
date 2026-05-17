#!/usr/bin/env python3

# reads observe committed values
# writes are buffered
# commit writes atomically
# replicate committed write batch
# store writer txn_id with each value
# with a brand-new tx_id, this local check often won’t catch much
# unless gossip also carries
# deps and remote transactions are inserted into the same dependency graph.

import asyncio
from collections import defaultdict
from maelstrom import Node

deps = {}
n = Node()
lock = asyncio.Lock()
k_v = {} # (value, writer_tx_id)
committed_txns = {} # tx_id -> writes dict: {key: value}
known_by_nbor = defaultdict(set) # "node_id": set of transaction_ids

counter = 0

def make_tx_id():
    global counter
    tx_id = f"{counter:020d}:{n.node_id}"
    counter += 1
    return tx_id

def apply_writes(tx_id, writes):
    """
    Apply a committed transaction's writes.

    tx_id gives deterministic last-writer-wins ordering so all nodes
    eventually converge even if gossip arrives in different orders.
    """
    for k, value in writes.items():
        old = k_v.get(k)

        if old is None:
            k_v[k] = (value, tx_id)
        else:
            old_value, old_tx_id = old

            if tx_id > old_tx_id:
                k_v[k] = (value, tx_id)


def reaches(start, target):
    """
    Return True if start can reach target in the dependency graph.

    Meaning:
        start -> ... -> target
    """
    stack = [start]
    visited = set()

    while stack:
        cur = stack.pop()

        if cur == target:
            return True

        if cur in visited:
            continue

        visited.add(cur)
        stack.extend(deps.get(cur, set()))

    return False

def would_create_cycle(tx_id, read_deps):
    """
    If this txn reads from transactions in read_deps,
    then we would add edges:

        tx_id -> dep

    A cycle exists if any dep can already reach tx_id.
    """
    for dep in read_deps:
        if reaches(dep, tx_id):
            return True

    return False


@n.handler
async def txn(req):
    req_txn = req.body["txn"]

    async with lock:
        tx_id = make_tx_id()
        writes = {}
        read_deps = set()

        for tx in req_txn:
            op = tx[0]
            k = tx[1]

            if op == "r":
                if k in writes:
                    tx[2] = writes[k]
                else:
                    entry = k_v.get(k)

                    if entry is None:
                        tx[2] = None
                    else:
                        value, writer_tx_id = entry
                        tx[2] = value

                        # This transaction depends on the transaction
                        # that produced the value it just read.
                        read_deps.add(writer_tx_id)
            elif op == "w":
                # Buffer writes. Do not expose them until commit.
                writes[k] = tx[2]
        if would_create_cycle(tx_id, read_deps):
            return {
                "type": "error",
                "code": 30,
                "text": "txn abort",
            }

        # Commit atomically.
        deps[tx_id] = read_deps
        committed_txns[tx_id] = writes
        apply_writes(tx_id, writes)

    return {
        "type": "txn_ok",
        "txn": req_txn,
    }


async def gossip_loop():
    sends = []

    async with lock:
        for nbor in n.node_ids:
            if nbor == n.node_id:
                continue

            batch = {}

            for tx_id, writes in committed_txns.items():
                if tx_id not in known_by_nbor[nbor]:
                    batch[tx_id] = writes

                if len(batch) >= 100:
                    break

            if batch:
                sends.append((nbor, batch))


    for nbor, batch  in sends:
        await n.send(nbor, {
            "type": "gossip",
            "txns": batch ,
        })


@n.handler
async def gossip(req):
    async with lock:
        incoming = req.body["txns"]

        known_by_nbor[req.src].update(incoming.keys())

        for tx_id, writes in incoming.items():
            if tx_id in committed_txns:
                continue

            committed_txns[tx_id] = writes
            apply_writes(tx_id, writes)


if __name__ == "__main__":
    n.every(1, gossip_loop)
    n.run()