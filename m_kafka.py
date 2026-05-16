#!/usr/bin/env python3

from maelstrom import Node

node = Node()

async def kv_read(key, default=None):
    res = await node.rpc("lin-kv", {
        "type": "read",
        "key": key,
    })

    if res["type"] == "read_ok":
        return res["value"]

    if res["type"] == "error" and res.get("code") == 20: # k not exists
        return default

    raise RuntimeError(f"lin-kv read failed: {res}")


async def kv_cas(key, old, new):
    res = await node.rpc("lin-kv", {
        "type": "cas",
        "key": key,
        "from": old,
        "to": new,
        "create_if_not_exists": True,
    })

    return res["type"] == "cas_ok"


@node.handler
async def send(req):
    k = req.body["key"]
    msg = req.body["msg"]

    log_key = f"log:{k}"

    while True:
        old_log = await kv_read(log_key, default=[])
        new_log = old_log + [msg]

        success = await kv_cas(log_key, old_log, new_log)
        if success:
            offset = len(old_log)
            return {
                "type": "send_ok",
                "offset": offset,
            }

@node.handler
async def poll(req):
    rpc_offsets = req.body["offsets"]
    res = {}

    for k, start in rpc_offsets.items():
        log = await kv_read(f"log:{k}", default=[])

        msgs = []

        for offset in range(start, len(log)):
            msgs.append([offset, log[offset]])
        res[k] = msgs

    return {
        "type": "poll_ok",
        "msgs": res
    }

@node.handler
async def commit_offsets(req):
    r_offsets = req.body["offsets"]

    for k, r_offset in r_offsets.items():
        commit_k = f"committed:{k}"

        while True:
            current = await kv_read(commit_k, default=-1)
            new_o = max(current, r_offset)

            # already committed or farther
            if new_o == current:
                break

            success = await kv_cas(commit_k, current, new_o)

            if success:
                break

    return {
        "type": "commit_offsets_ok",
    }

@node.handler
async def list_committed_offsets(req):
    keys = req.body["keys"]
    res = {}

    for k in keys:
        committed = await kv_read(f"committed:{k}", default=None)
        if committed is not None:
            res[k] = committed

    return {
        "type": "list_committed_offsets_ok",
        "offsets": res
    }

if __name__ == "__main__":
    node.run()