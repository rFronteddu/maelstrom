#!/usr/bin/env python3
import asyncio

from maelstrom import Node

n = Node()

# lock = asyncio.Lock()

k_v = {}

@n.handler
async def txn(req):
    req_txn = req.body["txn"]
    for tx in req_txn:
        op_name = tx[0]
        key = tx[1]
        if op_name == "r":
            tx[2] = k_v.get(key, "null")
        else:
            k_v[key] = tx[2]

    return {
        "type": "txn_ok",
        "txn": req_txn,
    }

if __name__ == "__main__":
    n.run()