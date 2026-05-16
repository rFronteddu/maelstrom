#!/usr/bin/env python3

from maelstrom import Node
from collections import defaultdict

node = Node()

next_offsets = defaultdict(int) # next offset to assign for ech log key
append_logs = defaultdict(dict) #append_logs[key][offset] = msg
committed_offsets = {} # committed_offsets[key] = highest commited offset

@node.handler
async def send(req):
    key = req.body["key"]
    msg = req.body["msg"]

    offset = next_offsets[key]
    next_offsets[key] += 1
    append_logs[key][offset] = msg

    return {
        "type": "send_ok",
        "offset": offset
    }

@node.handler
async def poll(req):
    req_offsets = req.body["offsets"]
    offsets_messages = {}

    for k, start_offset in req_offsets.items():
        msgs = []
        for offset in sorted(append_logs[k]):
            if offset >= start_offset:
                msgs.append([offset, append_logs[k][offset]])

        offsets_messages[k] = msgs

    return {
        "type": "poll_ok",
        "msgs": offsets_messages
    }

@node.handler
async def commit_offsets(req):
    r_commits = req.body["offsets"]

    for k, offset in r_commits.items():
        committed_offsets[k] = max(
            committed_offsets.get(k, -1),
            offset,
        )

    return {
        "type": "commit_offsets_ok",
    }

@node.handler
async def list_committed_offsets(req):
    # returns a map of committed offsets for a given set of logs.
    # Clients use this to figure out where to start consuming from in a given log.
    keys = req.body["keys"]

    result = {}

    for k in keys:
        if k in committed_offsets:
            result[k] = committed_offsets[k]

    return {
        "type": "list_committed_offsets_ok",
        "offsets": result,
    }

if __name__ == "__main__":
    node.run()
