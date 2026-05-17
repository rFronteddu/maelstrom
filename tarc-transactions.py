#!/usr/bin/env python3

import asyncio
from collections import defaultdict

from maelstrom import Node

n = Node()

lock = asyncio.Lock()

k_v = {}
seen = set() # set of transactions id we have seen
pending_transactions = defaultdict(list) # "transactionID" : [operations]
known_by_nbor = defaultdict(set) # "node_id": set of transaction_ids

counter = 0

@n.handler
async def txn(req):
    async with lock:
        req_txn = req.body["txn"]

        global counter
        tx_id = f"{n.node_id}:{counter}"

        for tx in req_txn:
            op_name = tx[0]
            key = tx[1]
            if op_name == "r":
                tx[2] = k_v.get(key, None)
            else:
                k_v[key] = tx[2]


        pending_transactions[tx_id] = req_txn
        counter+=1
        seen.add(tx_id)

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

            transactions = {}
            for tx_id, operations in pending_transactions.items():
                if tx_id not in known_by_nbor[nbor]:
                    transactions[tx_id] = operations

                    if len(transactions) >= 100:
                        break

            if transactions:
                sends.append((nbor, transactions))


    for nbor, transactions in sends:
        await n.send(nbor, {
            "type": "gossip",
            "txns": transactions,
        })

@n.handler
async def gossip(req):
    async with lock:
        txns = req.body["txns"]

        # The sender clearly knows every txn it sent us.
        known_by_nbor[req.src].update(txns.keys())

        for tx_id, operations in txns.items():
            if tx_id in seen:
                continue

            # add transactions to pending
            pending_transactions[tx_id] = operations
            seen.add(tx_id)


            for tx in operations:
                op_name = tx[0]
                key = tx[1]

                if op_name == "w":
                    k_v[key] = tx[2]
                # we can ignore reads


if __name__ == "__main__":
    n.every(0.1, gossip_loop)
    n.run()