#!/usr/bin/env python3

import asyncio
import hashlib
from collections import defaultdict
from maelstrom import Node

append_logs = defaultdict(list) #append_logs[key][offset] = msg
committed_offsets = {} # committed_offsets[key] = highest commited offset

loaded_logs = set()
loaded_commits = set()

key_locks = {}

node = Node()

def lock_for_key(k: str) -> asyncio.Lock: # blocks concurrent sends from interleaving
    lock = key_locks.get(k)
    if lock is None:
        lock = asyncio.Lock()
        key_locks[k] = lock
    return lock


def owner_for_key(key: str) -> str:
    digest = hashlib.sha256(key.encode()).hexdigest()
    value = int(digest, 16)
    index = value % len(node.node_ids)
    return node.node_ids[index]

async def seq_read(key, default=None):
    res = await node.rpc("seq-kv", {
        "type": "read",
        "key": key,
    })

    if res["type"] == "read_ok":
        return res["value"]

    if res["type"] == "error" and res.get("code") == 20:
        return default

    raise RuntimeError(f"seq-kv read failed: {res}")

async def seq_write(key, value):
    res = await node.rpc("seq-kv", {
        "type": "write",
        "key": key,
        "value": value,
    })

    if res["type"] != "write_ok":
        raise RuntimeError(f"seq-kv write failed: {res}")


async def load_log_if_needed(k: str): # avoids reading seq-kv if not needed
    if k not in loaded_logs:
        append_logs[k] = await seq_read(f"log:{k}", default=[])
        loaded_logs.add(k)

async def load_commit_if_needed(k: str):  # avoids reading seq-kv if not needed
    if k not in loaded_commits:
        committed = await seq_read(f"committed:{k}", default=None)

        if committed is not None:
            committed_offsets[k] = committed

        loaded_commits.add(k)


@node.handler
async def send(req):
    k = req.body["key"]
    msg = req.body["msg"]

    owner = owner_for_key(k)

    if owner != node.node_id:
       return await node.rpc(owner, {
            "type": "send",
            "key": k,
            "msg": msg,
        })

    async with lock_for_key(k):
        await load_log_if_needed(k)

        offset = len(append_logs[k])
        append_logs[k].append(msg)

        await seq_write(f"log:{k}", append_logs[k])

        return {
            "type": "send_ok",
            "offset": offset,
        }


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
                await load_log_if_needed(k)

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
                async with lock_for_key(k):
                    await load_commit_if_needed(k)

                    new_offset =  max(committed_offsets.get(k, -1), offset)
                    committed_offsets[k] = new_offset

                    await seq_write(f"committed:{k}", new_offset)
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
                await load_commit_if_needed(k)

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