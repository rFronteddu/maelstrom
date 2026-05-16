#!/usr/bin/env python3

from maelstrom import Node
from collections import defaultdict

node = Node()

offsets = defaultdict(int) # each log has an offset
append_log = defaultdict(map) # Each log is identified by a string key


@node.handler
async def send(req):
    key = req.body['key']
    msg = req.body['msg']

    append_log[key] = append_log[key] + 1
    append_log[key][offset] = msg
    return {
        "type": "send_ok",
        "offset": append_log[key]
    }

@node.handler
async def poll(req):
    offsets = req.body['offsets']
    for offset in offsets:
        pass
    return {
        "type": "poll_ok",
        "msgs": offsets_messages
    }

@node.handler
async def commit_offsets(req):
    c_offsets = req.body['offsets']

    return {
        "type": "commit_offsets_ok",
    }

@node.handler
async def list_committed_offsets(req):
    pass

if __name__ == "__main__":
    node.run()
