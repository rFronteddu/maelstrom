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
sed -i 's/\r//' echo.py
sed -i 's/\r//' uuid.py
sed -i 's/\r//' broadcast.py
sed -i 's/\r//' broadcast_periodic.py
sed -i 's/\r//' goc.py
```

## Broadcast
Implement a broadcast system that gossips messages between all nodes in the cluster. 

* broadcast.py Basic flood to all nodes once, replicate messages across a cluster that has no network partitions
* broadcast_periodic.py batched flood

```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 1 --time-limit 20 --rate 10
```

```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 5 --time-limit 20 --rate 10
```

```
./maelstrom/maelstrom test -w broadcast --bin ./broadcast.py --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

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
### goc
Each node writes in a key, only need to lock locally. Then read can sum all.
```
./maelstrom/maelstrom test -w g-counter --bin ./goc.py --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

## Single node kafka