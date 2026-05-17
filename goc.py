#!/usr/bin/env python3

from maelstrom import Node
import asyncio

node = Node()
counter = 0
lock = asyncio.Lock()

@node.handler
async def add(req):
    global counter

    async with lock:
        counter += req.body["delta"]

        await node.rpc("seq-kv", {
            "type": "write",
            "key": node.node_id,
            "value": counter
        })

    return {"type": "add_ok"}

@node.handler
async def read(req):
    total = 0
    for n in node.node_ids:
        try:
            res = await node.rpc("lin-kv", {
                "type": "read",
                "key": n
            })
            if res["type"] == "read_ok":
                total += res["value"]
        except Exception:
            pass
    return {
        "type": "read_ok",
        "value": total,
    }

if __name__ == "__main__":
    node.run()