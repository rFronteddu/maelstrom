# Maelstrom-py
This project contains solutions to the fly.io maelstrom challenges. The maelstrom.py 
file implements the node abstractions.

This was run on windows through WSL. Configure the terminal to connect to WSL and use the commands below to launch each test.

## Requirements
* python 3.14.4
* [maelstrom](https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.4)

## Echo
Your node will receive an "echo" message from Maelstrom, send a message with the same body back to the client but with a message type of "echo_ok".
```
./maelstrom/maelstrom test -w echo --bin ./echo.py --node-count 1 --time-limit 10
```

## UUID
Implement a globally-unique ID generation system that runs against Maelstrom’s unique-ids workload. 

IDs may be of any type–strings, booleans, integers, floats, arrays, etc.

```
./maelstrom/maelstrom test -w unique-ids --bin ./uuid.py --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

Remove windows terminator
```
sed -i 's/\r//' maelstrom.py
```

Make scripts executable:
```
chmod +x be_kafka.py
```

## Broadcast
Implement a broadcast system that gossips messages between all nodes in the cluster. 

* broadcast.py Basic flood to all nodes once, replicate messages across a cluster that has no network partitions
* broadcast_periodic.py batched flood

Single node
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 1 --time-limit 20 --rate 10
```

Multi node
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 5 --time-limit 20 --rate 10
```

Add network partition
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

Switch to gossip
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast_periodic.py --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

Solution must pass m-per-ops < 30, median lat < 400ms, max lat < 600 and must also complete with nemesis partition, set every period to 0.35
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast_periodic.py --node-count 25 --time-limit 20 --rate 100 --latency 100
```
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast_periodic.py --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition
```

Solution must pass m-per-ops < 32, median lat < 1s, max lat < 2s and must also complete with nemesis partition
```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast_periodic.py --node-count 25 --time-limit 20 --rate 100 --latency 100
```

## Grow-only Counter
* goc.py
Each node writes in a key, only need to lock locally. Then read can sum all.
```
./maelstrom/maelstrom test -w g-counter --bin ./goc.py --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

## Single node kafka
* single_kafka.py
```
./maelstrom/maelstrom test -w kafka --bin ./single_kafka.py --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
```

* m_kafka.py: move local structures to linear kv store
```
./maelstrom/maelstrom test -w kafka --bin ./m_kafka.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

* e_kafka.py
  * one writer per key: use stable hash (normal hash is randomized per process) to associate key to node

```
./maelstrom/maelstrom test -w kafka --bin ./e_kafka.py --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
./maelstrom/maelstrom test -w kafka --bin ./e_kafka.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

* be_kafka.py
  * storing in local memory is risky, use sharding + seq-kv
    * change send/poll for local node to lazy read and save in seq-kv

```
./maelstrom/maelstrom test -w kafka --bin ./be_kafka.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

## Totally available transactions
### Single-node, totally available transactions
```
./maelstrom/maelstrom test -w txn-rw-register --bin ./sn-transactions.py --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
```

### Multi-node, totally available transactions
```
./maelstrom/maelstrom test -w txn-rw-register --bin ./mn-transactions.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted

./maelstrom/maelstrom test -w txn-rw-register --bin ./mn-transactions.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
```

### Multi-node, totally available read committed transactions 

```
./maelstrom/maelstrom test -w txn-rw-register --bin ./tarc-transactions.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```

This version prevents cycle by outlawing cycles
```
./maelstrom/maelstrom test -w txn-rw-register --bin ./ta2rc-transactions.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```