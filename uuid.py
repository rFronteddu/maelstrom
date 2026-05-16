#!/usr/bin/env python3
import sys

from maelstrom import Node

node = Node()
counter = 0

def log(*args):
    print(*args, file=sys.stderr, flush=True)

@node.handler
async def generate(req):
    global counter
    uid = f"{node.node_id}:{counter}"
    counter += 1
    return {"type": "generate_ok", "id": uid}

if __name__ == "__main__":
    node.run()


