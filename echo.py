#!/usr/bin/env python3
from maelstrom import Node

node = Node()

@node.handler
async def echo(req):
    return {"type": "echo_ok", "id": req.body["echo"]}

if __name__ == "__main__":
    node.run()