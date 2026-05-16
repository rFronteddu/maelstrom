#!/usr/bin/env python3

from maelstrom import Node

node = Node()

@node.handler
async def send(req):
    key = req.body['key']
    msg = req.body['msg']

    return {
        "type": "send_ok",
        "offset": 1000
    }

@node.handler
async def poll(req):
    pass

if __name__ == "__main__":
    node.run()
