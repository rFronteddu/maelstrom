#!/usr/bin/env python3

from collections import defaultdict

from maelstrom import Node

node = Node()
messages = set()
known_by_peer = defaultdict(set) # messages we believe peer has

@node.handler
async def broadcast(req):
    msg = req.body["message"]

    if msg in messages:
        return {"type": "broadcast_ok"}

    messages.add(msg)

    if req.src.startswith("n"):
        known_by_peer[req.src].add(msg)
    # for n in node.node_ids:
    #     if n != node.node_id:
    #         await node.send(n, {
    #             "type": "gossip",
    #             "messages": [msg],
    #         })

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

@node.handler
async def gossip(req):
    src = req.src
    body_messages = req.body["messages"]
    known_by_peer[src].update(body_messages) # They sent these, so they definitely know them.
    messages.update(body_messages)


async def gossip_loop():
    if node.node_id is None:
        return

    snapshot = sorted(messages) # to send in order

    for nbor in node.node_ids:
        if nbor == node.node_id:
            continue

        known = known_by_peer[nbor]
        batch = []
        for msg in snapshot:
            if msg not in known:
                batch.append(msg)

                if len(batch) >= 200:
                    break
        if not batch:
            continue

        # use a snapshot of messages to avoid locking
        await node.send(nbor, {
            "type": "gossip",
            "messages": batch,
        })



if __name__ == "__main__":
    node.every(0.50, gossip_loop)
    node.run()