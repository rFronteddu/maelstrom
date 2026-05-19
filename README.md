# Maelstrom-py

A small Python playground for learning distributed systems through two complementary environments:

1. **Fly.io Maelstrom challenge solutions** built on top of `maelstrom.py`, an `asyncio` message-processing helper.
2. **Tiny in-process distributed-system simulations** built on top of `tiny_test_lib.py`, a lightweight threading-based test harness.

The repository explores core ideas such as gossip, replication, counters, partition tolerance, Kafka-style logs, and transactional key-value workloads.

---

## Repository layout

### Maelstrom workloads

| File | Purpose |
| --- | --- |
| `maelstrom.py` | Async helper layer for Maelstrom-compatible nodes |
| `echo.py` | Echo workload |
| `m_uuid.py` | Globally unique ID generation |
| `broadcast.py` | Basic broadcast / flooding implementation |
| `broadcast_periodic.py` | Batched periodic gossip broadcast |
| `goc.py` | Grow-only counter |
| `single_kafka.py` | Single-node Kafka-style log |
| `m_kafka.py` | Multi-node Kafka using a linearizable KV store |
| `e_kafka.py` | Key-partitioned Kafka variant with stable hashing |
| `be_kafka.py` | Sharded Kafka variant backed by sequential KV |
| `sn-transactions.py` | Single-node transactional register |
| `mn-transactions.py` | Multi-node totally available transactions |
| `tarc-transactions.py` | Totally available read-committed transactions |
| `ta2rc-transactions.py` | Read-committed variant that prevents dependency cycles |

### Tiny simulation library

| File | Purpose |
| --- | --- |
| `tiny_test_lib.py` | Threading-based local distributed-system harness |
| `tiny_gossip.py` | Gossip propagation |
| `tiny_periodic_gossip.py` | Periodic batched gossip |
| `tiny_gcounter.py` | Grow-only counter |
| `tiny_leader_counter.py` | Leader-coordinated counter |
| `tiny_leader_failover_counter.py` | Leader counter with failover |
| `tiny_replicated_kv.py` | Replicated key-value store |
| `tiny_quorum_kv.py` | Quorum-style key-value store |
| `tiny_transaction_kv.py` | Transactional key-value experiment |
| `tiny_or_set.py` | Observed-remove set / CRDT-style experiment |

---

## Requirements

This repository was developed and tested from **Windows via WSL**.

### Tooling

- Python `3.14.4` in the current development setup
- [Maelstrom](https://github.com/jepsen-io/maelstrom)
- A Unix-like shell environment, preferably WSL on Windows or Linux/macOS

---

## Setup

Clone the repository:

```bash
git clone https://github.com/rFronteddu/maelstrom.git
cd maelstrom
```

Download or place the Maelstrom binary under:

```text
./maelstrom/maelstrom
```

On WSL or Linux, make scripts executable when needed:

```bash
chmod +x *.py
```

If a file picked up Windows line endings and fails to execute, normalize it:

```bash
sed -i 's/\r$//' maelstrom.py
```

---

## Maelstrom workloads

The following commands assume:

- You are running them from the repository root
- The Maelstrom binary is available at `./maelstrom/maelstrom`

---

### 1. Echo

The node receives an `echo` message and returns the same payload in an `echo_ok` response.

```bash
./maelstrom/maelstrom test \
  -w echo \
  --bin ./echo.py \
  --node-count 1 \
  --time-limit 10
```

---

### 2. Unique IDs

Generate globally unique IDs across a partitionable cluster.

```bash
./maelstrom/maelstrom test \
  -w unique-ids \
  --bin ./m_uuid.py \
  --time-limit 30 \
  --rate 1000 \
  --node-count 3 \
  --availability total \
  --nemesis partition
```

---

### 3. Broadcast

Broadcast messages across a cluster.

#### Implementations

- `broadcast.py` — basic flood-style dissemination
- `broadcast_periodic.py` — batched periodic gossip for better efficiency under load and partitions

#### Single node

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast.py \
  --node-count 1 \
  --time-limit 20 \
  --rate 10
```

#### Multi-node broadcast

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast.py \
  --node-count 5 \
  --time-limit 20 \
  --rate 10
```

#### Multi-node broadcast with partitions

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast.py \
  --node-count 5 \
  --time-limit 20 \
  --rate 10 \
  --nemesis partition
```

#### Gossip-based broadcast with partitions

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast_periodic.py \
  --node-count 5 \
  --time-limit 20 \
  --rate 10 \
  --nemesis partition
```

#### Larger broadcast stress tests

The periodic gossip implementation was tuned against Maelstrom's higher-scale broadcast workloads. A period of roughly `0.35s` is noted in the current experiments.

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast_periodic.py \
  --node-count 25 \
  --time-limit 20 \
  --rate 100 \
  --latency 100
```

```bash
./maelstrom/maelstrom test \
  -w broadcast \
  --bin ./broadcast_periodic.py \
  --node-count 25 \
  --time-limit 20 \
  --rate 100 \
  --latency 100 \
  --nemesis partition
```

Target constraints from the current notes:

- Message overhead below roughly `30–32 messages/op`
- Median latency below approximately `400 ms–1 s`, depending on the challenge variant
- Maximum latency below approximately `600 ms–2 s`
- Successful completion under network partitions

---

### 4. Grow-only counter

`goc.py` stores one partial count per node. Each node only locks its own local contribution, while reads aggregate the full counter value.

```bash
./maelstrom/maelstrom test \
  -w g-counter \
  --bin ./goc.py \
  --node-count 3 \
  --rate 100 \
  --time-limit 20 \
  --nemesis partition
```

---

### 5. Kafka-style log workloads

This section explores progressively more distributed versions of the Kafka workload.

#### Single-node Kafka

```bash
./maelstrom/maelstrom test \
  -w kafka \
  --bin ./single_kafka.py \
  --node-count 1 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

#### Multi-node Kafka using linearizable KV

`m_kafka.py` moves shared state from local in-memory structures into Maelstrom's linearizable key-value store.

```bash
./maelstrom/maelstrom test \
  -w kafka \
  --bin ./m_kafka.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

#### Key-partitioned Kafka

`e_kafka.py` assigns each key to a stable owner node. It avoids Python's built-in randomized process hash by using a stable hashing strategy.

```bash
./maelstrom/maelstrom test \
  -w kafka \
  --bin ./e_kafka.py \
  --node-count 1 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

```bash
./maelstrom/maelstrom test \
  -w kafka \
  --bin ./e_kafka.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

#### Sharded Kafka backed by sequential KV

`be_kafka.py` reduces reliance on local memory by combining sharding with Maelstrom's sequential KV store. Local-node reads are handled lazily, while state is persisted through KV-backed coordination.

```bash
./maelstrom/maelstrom test \
  -w kafka \
  --bin ./be_kafka.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

---

### 6. Transaction workloads

#### Single-node totally available transactions

```bash
./maelstrom/maelstrom test \
  -w txn-rw-register \
  --bin ./sn-transactions.py \
  --node-count 1 \
  --time-limit 20 \
  --rate 1000 \
  --concurrency 2n \
  --consistency-models read-uncommitted \
  --availability total
```

#### Multi-node totally available transactions

```bash
./maelstrom/maelstrom test \
  -w txn-rw-register \
  --bin ./mn-transactions.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  --consistency-models read-uncommitted
```

```bash
./maelstrom/maelstrom test \
  -w txn-rw-register \
  --bin ./mn-transactions.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  --consistency-models read-uncommitted \
  --availability total \
  --nemesis partition
```

#### Multi-node, totally available read-committed transactions

```bash
./maelstrom/maelstrom test \
  -w txn-rw-register \
  --bin ./tarc-transactions.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  --consistency-models read-committed \
  --availability total \
  --nemesis partition
```

`ta2rc-transactions.py` is a stricter read-committed variant that avoids cyclic dependency patterns.

```bash
./maelstrom/maelstrom test \
  -w txn-rw-register \
  --bin ./ta2rc-transactions.py \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000 \
  --consistency-models read-committed \
  --availability total \
  --nemesis partition
```

---

## Tiny local simulation library

The `tiny_*` files do **not** require Maelstrom. They are local experiments built on `tiny_test_lib.py` for testing distributed-system patterns in a smaller, easier-to-inspect environment.

These scripts cover:

- Gossip dissemination
- Periodic reconciliation
- Grow-only counters
- Leader-based coordination
- Leader failover
- Replicated and quorum-style KV stores
- Transactional KV sketches
- Set-based CRDT ideas

Run them directly with Python, for example:

```bash
python tiny_gossip.py
python tiny_gcounter.py
python tiny_replicated_kv.py
```

---

## Topology helpers

A few helper patterns are useful when assigning peers or routing replication traffic.

### Binary tree overlay

```python
def compute_tree(
    node_id: str,
    node_ids: list[str],
) -> tuple[str | None, list[str]]:
    ordered = sorted(node_ids)
    i = ordered.index(node_id)

    if i == 0:
        parent = None
    else:
        parent = ordered[(i - 1) // 2]

    children = []

    left_index = 2 * i + 1
    if left_index < len(ordered):
        children.append(ordered[left_index])

    right_index = 2 * i + 2
    if right_index < len(ordered):
        children.append(ordered[right_index])

    return parent, children
```

### Ring neighbors

```python
def ring_from_neighbors(
    node_id: str,
    node_ids: list[str],
) -> tuple[str, str]:
    ordered = sorted(node_ids)
    i = ordered.index(node_id)
    n = len(ordered)

    previous_node = ordered[(i - 1) % n]
    next_node = ordered[(i + 1) % n]

    return previous_node, next_node
```

### Next `k` neighbors

```python
def next_k_neighbors(
    node_id: str,
    node_ids: list[str],
    k: int = 3,
) -> list[str]:
    ordered = sorted(node_ids)
    i = ordered.index(node_id)
    n = len(ordered)

    neighbors = []

    for offset in range(1, k + 1):
        neighbor = ordered[(i + offset) % n]
        if neighbor != node_id:
            neighbors.append(neighbor)

    return neighbors
```

---

## Why this repository exists

This repository is a learning-oriented sandbox for experimenting with distributed-system behavior under:

- Concurrency
- Message loss and reordering
- Network partitions
- Replication lag
- Coordination trade-offs
- Availability vs. consistency constraints

The Maelstrom programs provide externally validated workloads, while the tiny local simulations make it easier to iterate on ideas quickly before applying them to a full challenge.

---
