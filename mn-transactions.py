#!/usr/bin/env python3
import asyncio

from maelstrom import Node

n = Node()

lock = asyncio.Lock()

k_v = {}

@n.handler
async def txn(req):
    async with lock:
        req_txn = req.body["txn"]
        for tx in req_txn:
            op_name = tx[0]
            key = tx[1]
            if op_name == "r":
                tx[2] = k_v.get(key, None)
            else:
                k_v[key] = tx[2]


        for nbor in n.node_ids:
            if nbor != n.node_id:
                await n.send(nbor, {
                    "type": "gossip",
                    "txn": req_txn,
                })

    return {
        "type": "txn_ok",
        "txn": req_txn,
    }



@n.handler
async def gossip(req):
    async with lock:
        req_txn = req.body["txn"]
        for tx in req_txn:
            op_name = tx[0]
            key = tx[1]
            if op_name == "w":
                k_v[key] = tx[2]


if __name__ == "__main__":
    n.run()