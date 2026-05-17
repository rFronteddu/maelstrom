#!/usr/bin/env python3

import asyncio
from collections import defaultdict
from typing import Any

from maelstrom import Node
import hashlib

n = Node()

locks = {}

committed_logs = defaultdict(int)
append_logs = defaultdict(list)
loaded_logs = set()
loaded_commits = set()

def lock_from_key(key: str) -> asyncio.Lock:
    if key not in locks:
        locks[key] = asyncio.Lock()
    return locks[key]

def owner_from_key(key: str) -> str:
    digest = hashlib.sha256(key.encode()).hexdigest()
    v = int(digest, 16)
    return n.node_ids[v % len(n.node_ids)]

async def seq_read(key: str, default: Any = None) -> Any:
    res = await n.rpc("seq-kv", {
        "type": "read",
        "key": key,
    })

    if res["type"] == "read_ok":
        return res["value"]

    return default

async def seq_write(k, v):
    res = await n.rpc("seq-kv", {
        "type": "write",
        "key": k,
        "value": v,
    })

    if res["type"] != "write_ok":
        raise RuntimeError(f"seq-kv write failed: {res}")

async def lazy_load_logs(key: str):
    if key not in loaded_logs:
        res = await seq_read(f"log:{key}", []) # []
        append_logs[key] = res
        loaded_logs.add(key)

async def lazy_load_commits(key: str):
    if key not in loaded_commits:
        res = await seq_read(f"commit:{key}", None)
        if res is not None:
            committed_logs[key] = res

        loaded_commits.add(key) # I have read it and it's none

@n.handler
async def send(req):
    k = req.body["key"]
    msg = req.body["msg"]
    owner = owner_from_key(k)

    if owner != n.node_id:
        return await n.rpc(owner, {
            "type": "send",
            "key": k,
            "msg": msg,
        })

    async with lock_from_key(k):
        await lazy_load_logs(k)
        offset = len(append_logs[k])
        append_logs[k].append(msg)
        await seq_write(f"log:{k}", append_logs[k])

        return {
            "type": "send_ok",
            "offset": offset
        }

@n.handler
async def poll(req):
    req_offsets = req.body["offsets"]

    owner_to_offsets = defaultdict(dict) # "n1": { "k1": 1000, "k2": 2000 }
    for k, offset in req_offsets.items():
        owner = owner_from_key(k)
        owner_to_offsets[owner][k] = offset

    msgs = defaultdict(list)
    remote_calls = []

    for owner, offsets in owner_to_offsets.items():
        if owner == n.node_id:
            for k, start in offsets.items():
                async with lock_from_key(k):
                    await lazy_load_logs(k)

                    for offset in range(start, len(append_logs[k])):
                        msgs[k].append([offset, append_logs[k][offset]])
        else:
            remote_calls.append(n.rpc(owner, {
                "type": "poll",
                "offsets": offsets,
            }))

    res = await asyncio.gather(*remote_calls)
    for owner_res in res:
        msgs.update(owner_res["msgs"])

    return {
        "type": "poll_ok",
        "msgs": dict(msgs),
    }

@n.handler
async def commit_offsets(req):
    req_offsets = req.body["offsets"]

    owner_to_offsets = defaultdict(dict)
    for k, offset in req_offsets.items():
        owner = owner_from_key(k)
        owner_to_offsets[owner][k] = offset

    remote_calls = []

    for owner, offsets in owner_to_offsets.items():
        if owner == n.node_id:

            for k, commit in offsets.items():
                async with lock_from_key(k):
                    await lazy_load_commits(k)
                    committed_logs[k] = max(committed_logs.get(k, -1), commit)
                    await seq_write(f"commit:{k}", committed_logs[k])
        else:
            remote_calls.append(n.rpc(owner, {
                "type": "commit_offsets",
                "offsets": offsets,
            }))

    await asyncio.gather(*remote_calls)


    return {
        "type": "commit_offsets_ok",
    }

@n.handler
async def list_committed_offsets(req):
    keys = req.body["keys"]

    offsets = {}

    owner_to_keys = defaultdict(list)
    for k in keys:
        owner = owner_from_key(k)
        owner_to_keys[owner].append(k)

    remote_calls = []
    for owner, owner_keys in owner_to_keys.items():
        if owner != n.node_id:
            remote_calls.append(n.rpc(owner, {
                "type": "list_committed_offsets",
                "keys": owner_keys,
            }))
        else:
            for k in owner_keys:
                async with lock_from_key(k):
                    await lazy_load_commits(k)
                    if k in committed_logs:
                        offsets[k] = committed_logs[k]


    res = await asyncio.gather(*remote_calls)
    for owner_res in res:
        offsets.update(owner_res["offsets"])


    return {
        "type": "list_committed_offsets_ok",
        "offsets": offsets
    }

if __name__ == "__main__":
    n.run()
