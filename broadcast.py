#!/usr/bin/env python3
import sys

from maelstrom import Node

node = Node()
messages = set()

def log(*args):
    print(*args, file=sys.stderr, flush=True)

@node.handler
async def broadcast(req):
    """Basic flood to all nodes once"""
    msg = req.body["message"]

    if msg in messages:
        return {"type": "broadcast_ok"}

    messages.add(msg)

    for n in node.node_ids:
        if n!= node.node_id:
            await node.send(n, {
                "type": "broadcast",
                "message": msg,
            })

    return {"type": "broadcast_ok"}

@node.handler
async def read(req):
    return {
        "type": "read_ok",
        "messages": list(messages)
    }

@node.handler
async def topology(req):
    return {
        "type": "topology_ok",
    }


if __name__ == "__main__":
    node.run()