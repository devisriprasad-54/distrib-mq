# distrib-mq

A distributed message queue built from scratch in Go — inspired by Apache Kafka's core architecture.

No external dependencies. No managed infrastructure. Just Go, TCP, and disk.

---

## Architecture
```
Producer
    ↓
Leader Broker (port 9090)
    ├── Partition 0  →  orders-0.log + orders-0.index
    ├── Partition 1  →  orders-1.log + orders-1.index
    └── Partition 2  →  orders-2.log + orders-2.index
         ↓ replicates to
Follower 1 (port 9091)
Follower 2 (port 9092)
         ↓ consumed by
Consumer Group (order-processors)
    ├── consumerA → partition 0
    ├── consumerB → partition 1
    └── consumerC → partition 2
```

## Features

- **Append-only log** — sequential disk writes with binary index for O(1) reads by offset
- **Consistent hash partitioning** — FNV-1a hashing ensures same key always routes to same partition
- **Leader-follower replication** — followers continuously poll leader, data survives broker crashes
- **Persistent index** — byte positions stored in binary index files, offsets survive restarts
- **Consumer groups** — automatic partition assignment, rebalances on failure
- **Heartbeat detection** — dead consumers trigger rebalance within 60 seconds
- **Concurrent producers** — goroutine-per-connection, mutex-protected log writes

---

## Benchmark

Tested on MacBook Pro (Apple Silicon), localhost:

| Metric | Single Producer | 3 Concurrent Producers |
|--------|----------------|----------------------|
| Throughput | 43,669 msg/sec | ~120,000 msg/sec combined |
| Latency p50 | 0.02ms | 0.02ms |
| Latency p99 | 0.04ms | 0.05ms |
| Latency max | 4.14ms | 0.37ms |

---

## Quick Start

**Requirements:** Go 1.21+
```bash
git clone https://github.com/devisriprasad-54/distrib-mq
cd distrib-mq
mkdir data
```

**Start the leader broker:**
```bash
go run server/main.go server/handler.go server/log.go server/group.go -id=0 -port=9090
```

**Start followers:**
```bash
go run server/main.go server/handler.go server/log.go server/group.go -id=1 -port=9091 -leader=localhost:9090
go run server/main.go server/handler.go server/log.go server/group.go -id=2 -port=9092 -leader=localhost:9090
```

**Start consumers:**
```bash
go run consumer/main.go -group=order-processor -id=consumerA -topic=orders
go run consumer/main.go -group=order-processor -id=consumerB -topic=orders
go run consumer/main.go -group=order-processor -id=consumerC -topic=orders
```

**Send messages:**
```bash
telnet localhost 9090
SEND orders user-123 order-placed-500
READ orders 1 0
```

**Run benchmark:**
```bash
go run producer/main.go -messages=100000
```

---

## Protocol
```
SEND <topic> <key> <message>          → OK <partition> <offset>
READ <topic> <partition> <offset>     → MSG <message> | EMPTY
JOIN <groupID> <consumerID>           → ASSIGNED <partition>
PING <groupID> <consumerID>           → PONG
PARTITIONS <topic>                    → OK <count>
```

---

## Layers Built

| Layer | What | Key Concept |
|-------|------|-------------|
| 1 | TCP server + append-only log | Sequential writes, offset indexing, binary persistence |
| 2 | Consistent hash partitioning | FNV-1a, hot partitions, per-key ordering guarantee |
| 3 | Leader-follower replication | Fault tolerance, ISR, acks tradeoff |
| 4 | Consumer groups | Heartbeats, rebalancing, exactly-once consumption |
| 5 | Benchmarking | Throughput, p50/p99 latency, concurrent load testing |
