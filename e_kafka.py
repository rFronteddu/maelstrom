#!/usr/bin/env python3

import asyncio
import hashlib
from collections import defaultdict
from maelstrom import Node

append_logs = defaultdict(list) #append_logs[key][offset] = msg
committed_offsets = {} # committed_offsets[key] = highest commited offset
node = Node()

def owner_for_key(key: str) -> str:
    digest = hashlib.sha256(key.encode()).hexdigest()
    value = int(digest, 16)
    index = value % len(node.node_ids)
    return node.node_ids[index]


@node.handler
async def send(req):
    k = req.body["key"]
    msg = req.body["msg"]

    owner = owner_for_key(k)
    if owner == node.node_id:
        # we are the owner
        offset = len(append_logs[k])
        append_logs[k].append(msg)

        return {
            "type": "send_ok",
            "offset": offset,
        }
    else:
        res = await node.rpc(owner, {
            "type": "send",
            "key": k,
            "msg": msg,
        })
        return res

@node.handler
async def poll(req):
    req_offset = req.body["offsets"]
    res = {}

    owner_to_offsets = defaultdict(dict)

    for k, start in req_offset.items():
        owner = owner_for_key(k)
        owner_to_offsets[owner][k] = start

    remote_calls = []

    for owner, offsets_for_owner in owner_to_offsets.items():
        if owner == node.node_id:
            for k, start in offsets_for_owner.items():
                msgs = []

                for offset in range(start, len(append_logs[k])):
                    msgs.append([offset, append_logs[k][offset]])

                res[k] = msgs
        else:
            # same pattern can be applied to commit_offsets/list_commited_offsets
            remote_calls.append(node.rpc(owner, {
                    "type": "poll",
                    "offsets": offsets_for_owner,
                })
            )

    remote_results = await asyncio.gather(*remote_calls)
    for owner_res in remote_results:
        res.update(owner_res["msgs"])

    return {
        "type": "poll_ok",
        "msgs": res,
    }

@node.handler
async def commit_offsets(req):
    req_offsets = req.body["offsets"]

    owner_to_offsets = defaultdict(dict)

    for k, offset in req_offsets.items():
        owner = owner_for_key(k)
        owner_to_offsets[owner][k] = offset

    for owner, offsets_for_owner in owner_to_offsets.items():
        if owner == node.node_id:
            for k, offset in offsets_for_owner.items():
                committed_offsets[k] = max(committed_offsets.get(k, -1), offset)
        else:
            await node.rpc(owner, {
                "type": "commit_offsets",
                "offsets": offsets_for_owner,
            })

    return {
        "type": "commit_offsets_ok",
    }

@node.handler
async def list_committed_offsets(req):
    req_keys = req.body["keys"]
    owner_to_keys = defaultdict(list)

    for k in req_keys:
        owner = owner_for_key(k)
        owner_to_keys[owner].append(k)

    result = {}

    for owner, keys in owner_to_keys.items():
        if owner == node.node_id:
            for k in keys:
                if k in committed_offsets:
                    result[k] = committed_offsets[k]
        else:
            res = await node.rpc(owner, {
                "type": "list_committed_offsets",
                "keys": keys,
            })
            result.update(res["offsets"])

    return {
        "type": "list_committed_offsets_ok",
        "offsets": result,
    }

if __name__ == "__main__":
    node.run()