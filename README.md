# lau-distributed-systems

A Rust library implementing core **distributed systems** algorithms: consensus (Paxos, Raft), consistency models, vector clocks, quorum systems, gossip protocols, consistent hashing, leader election, CAP theorem simulation, and multi-agent fleet coordination.

---

## What This Does

| Module | What you get |
|---|---|
| `vector_clock` | Vector clocks, causal ordering, conflict resolution (LWW, merge) |
| `consensus::paxos` | Full Paxos: proposer, acceptor, learner; simulated multi-node runs |
| `consensus::raft` | Raft consensus: leader election, log replication, state machine |
| `consistency` | Strong / Eventual / Causal consistency models; replica merging |
| `quorum` | Quorum reads/writes with R + W > N; fault-tolerance analysis |
| `gossip` | Gossip protocol with push-pull exchange, convergence tracking |
| `hash_ring` | Consistent hashing ring with virtual nodes; key distribution analysis |
| `leader_election` | Bully and Ring leader election algorithms |
| `cap` | CAP theorem simulation: latency matrices, consistency/availability tradeoffs |
| `coordination` | Multi-agent fleet with Raft-backed state proposals and vector clocks |

---

## Key Idea

Distributed systems must handle partial failure, network partitions, and replication lag. This library provides **simulable** implementations of the fundamental algorithms—you can spin up a cluster of virtual nodes, inject failures, run consensus, and observe convergence—all in a single process, without networking.

---

## Install

```toml
[dependencies]
lau-distributed-systems = { git = "https://github.com/SuperInstance/lau-distributed-systems" }
```

Requires Rust 2021 edition.

---

## Quick Start

### Vector clocks and causal ordering

```rust
use lau_distributed_systems::vector_clock::VectorClock;

let mut vc1 = VectorClock::new();
vc1.increment("node_a");
vc1.increment("node_a");

let mut vc2 = VectorClock::new();
vc2.increment("node_b");

assert!(vc1.is_concurrent_with(&vc2)); // neither happened-before the other

let mut vc3 = vc1.clone();
vc3.increment("node_a");
assert!(vc1.happened_before(&vc3)); // vc1 < vc3
```

### Run Paxos consensus

```rust
use lau_distributed_systems::consensus::paxos::{PaxosNode, run_paxos};

let mut nodes = vec![
    PaxosNode::new("n0".into(), 3),
    PaxosNode::new("n1".into(), 3),
    PaxosNode::new("n2".into(), 3),
];
run_paxos(&mut nodes, 0, "my_value");
assert_eq!(nodes[0].learned_value, Some("my_value".to_string()));
```

### Raft leader election and log replication

```rust
use lau_distributed_systems::consensus::raft::{RaftNode, simulate_election, replicate_entry};

let ids: Vec<String> = (0..3).map(|i| format!("n{}", i)).collect();
let mut nodes: Vec<RaftNode> = ids.iter().map(|id| {
    let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
    RaftNode::new(id.clone(), peers)
}).collect();

simulate_election(&mut nodes, 0);
assert_eq!(nodes[0].role, lau_distributed_systems::consensus::raft::NodeRole::Leader);

replicate_entry(&mut nodes, 0, "set x=42");
assert_eq!(nodes[1].log[0].command, "set x=42");
```

### Quorum reads and writes with failures

```rust
use lau_distributed_systems::quorum::QuorumSystem;

let mut sys = QuorumSystem::new(5);
sys.write("key", "value");
assert_eq!(sys.read("key"), Some("value".to_string()));

sys.kill_node(0);
sys.kill_node(1);
assert!(sys.write("key", "value2")); // still works with 3/5 alive
```

### Gossip protocol convergence

```rust
use lau_distributed_systems::gossip::GossipNetwork;

let mut net = GossipNetwork::new(20, 3);
net.seed_rumor(0, "breaking_news");
let rounds = net.run_until_convergence(50);
assert!(rounds <= 50);
// All 20 nodes now know "breaking_news"
```

### Consistent hashing

```rust
use lau_distributed_systems::hash_ring::HashRing;

let mut ring = HashRing::new(150); // 150 virtual nodes per physical node
ring.add_node("server_a");
ring.add_node("server_b");
ring.add_node("server_c");

assert_eq!(ring.get_node("user:42"), ring.get_node("user:42")); // deterministic

let dist = ring.distribution(&["k1", "k2", "k3", "k4", "k5"]);
// Balanced distribution across nodes
```

### Multi-agent fleet coordination

```rust
use lau_distributed_systems::coordination::AgentFleet;

let mut fleet = AgentFleet::new(5);
fleet.elect_leader(2);
fleet.propose_state("config", "v2");
fleet.propose_state("mode", "active");

for agent in &fleet.agents {
    assert_eq!(agent.get("config"), Some("v2"));
}
```

---

## API Reference

### `vector_clock`

| Type / Function | Description |
|---|---|
| `VectorClock` | HashMap<String, u64> with `increment()`, `get()`, `merge()`. |
| `compare()` | Returns `Some(true)` if self < other, `Some(false)` if self ≥ other, `None` if concurrent. |
| `happened_before()`, `is_concurrent_with()` | Causal relation queries. |
| `ConflictEntry` | Key with multiple (value, clock) pairs. |
| `resolve_lww()` | Last-writer-wins: pick highest clock sum. |
| `resolve_merge()` | Merge all clocks, keep first value. |

### `consensus::paxos`

| Type / Function | Description |
|---|---|
| `PaxosMessage` | `Prepare`, `Promise`, `Accept`, `Accepted` variants. |
| `AcceptorState` | Tracks `promised_number` and `accepted_proposal`. `receive_prepare()`, `receive_accept()`. |
| `ProposerState` | Tracks proposal number, received promises. `prepare()`, `receive_promise()`. |
| `PaxosNode` | Combined proposer + acceptor + learner. `propose()`, `receive()`. |
| `run_paxos(nodes, proposer_idx, value)` | Simulates full Paxos rounds across all nodes. |

### `consensus::raft`

| Type / Function | Description |
|---|---|
| `NodeRole` | `Follower`, `Candidate`, `Leader`. |
| `RaftMessage` | `RequestVote`, `VoteResponse`, `AppendEntries`, `AppendResponse`. |
| `RaftNode` | Full Raft state: term, log, commit index, state machine. `start_election()`, `handle_request_vote()`, `handle_vote_response()`, `append_entries()`, `leader_append()`. |
| `simulate_election()` | Run an election across a cluster. |
| `replicate_entry()` | Replicate a log entry from leader to followers. |

### `consistency`

| Type / Function | Description |
|---|---|
| `ConsistencyModel` | `Strong`, `Eventual`, `Causal`. |
| `DataStore` | Key-value store with pluggable consistency. `write()`, `read()`. |
| `VersionedValue` | Value + version + timestamp + vector clock. |
| `merge_replicas()` | LWW merge across replicas for a key. |
| `happened_before()`, `are_concurrent()` | Vector clock ordering on HashMaps. |

### `quorum`

| Type / Function | Description |
|---|---|
| `QuorumConfig` | N, R, W with `is_valid()` (R+W>N), fault tolerance. |
| `QuorumNode` | Node with data HashMap and alive flag. |
| `QuorumSystem` | Cluster with `write()`, `read()`, `kill_node()`. |

### `gossip`

| Type / Function | Description |
|---|---|
| `GossipNode` | Knows rumors (HashMap), `add_rumor()`, `exchange()` (push-pull). |
| `GossipNetwork` | N nodes with fanout. `seed_rumor()`, `round()`, `run_until_convergence()`, `convergence_ratio()`. |

### `hash_ring`

| Type / Function | Description |
|---|---|
| `VirtualNode` | Position on the ring + physical node name. |
| `HashRing` | Sorted ring of virtual nodes. `add_node()`, `remove_node()`, `get_node()`, `distribution()`. |

### `leader_election`

| Type / Function | Description |
|---|---|
| `BullyNode` | Bully algorithm: highest-ID alive node wins. `start_election()`. |
| `RingNode` | Ring algorithm: election message circulates; highest ID wins. `start_election()`. |

### `cap`

| Type / Function | Description |
|---|---|
| `CapChoice` | `Consistency` or `Availability`. |
| `CapSimulation` | Tracks scores during partitions/latency. `simulate_partition()`, `simulate_latency()`. |
| `latency_matrix(nodes, base)` | Generates inter-node latency matrix. |
| `analyze_tradeoff()` | Quick CAP analysis. |

### `coordination`

| Type / Function | Description |
|---|---|
| `Agent` | Agent with state HashMap + vector clock + role. `set()`, `get()`. |
| `AgentRole` | `Leader`, `Follower`, `Candidate`. |
| `AgentFleet` | N agents + N Raft nodes + N data stores. `elect_leader()`, `propose_state()`, `synchronize()`, `leader()`. |

---

## How It Works

The library is structured as independent modules that can be composed:

1. **Vector clocks** track causal ordering of events across nodes. Each node increments its own counter; merging takes element-wise maxima. Two clocks are concurrent if neither dominates the other.

2. **Paxos** implements the classic two-phase protocol. A proposer broadcasts `Prepare(n)`, collect `Promise` responses from a quorum, then broadcasts `Accept(proposal)`. Acceptors reject stale proposals. Safety is guaranteed: once a value is chosen, no other value can be.

3. **Raft** separates leader election (randomized timeouts, vote requests) from log replication (AppendEntries with consistency checks). The leader commits entries once a majority acknowledges. State machine application follows commit index.

4. **Consistency models** differ in how writes are tracked: Strong uses monotonic versions, Eventual uses last-writer-wins, Causal tracks vector clocks per key.

5. **Quorum systems** enforce R + W > N so that any read quorum overlaps at least one node from the latest write quorum. Dead nodes reduce available replicas.

6. **Gossip** uses push-pull epidemic spreading: each round, every node exchanges rumors with `fanout` random peers. Convergence is O(log N) rounds with high probability.

7. **Consistent hashing** maps both nodes and keys to positions on a hash ring. Virtual nodes smooth out the distribution. Adding/removing a node only moves keys near its positions.

8. **Leader election** implements Bully (highest-ID alive wins) and Ring (election message circulates, highest participant ID wins).

9. **CAP simulation** models the tradeoff: during a partition, choosing consistency blocks unavailable replicas (low availability score), choosing availability allows stale reads (low consistency score).

10. **Coordination** ties it together: an `AgentFleet` runs Raft internally for leader election and state replication, while each agent tracks its own vector clock for causal reasoning.

---

## The Math

### Vector Clocks and Causal Order
A vector clock VC maps each process to a counter: VC[i] is the count of events at process i. For two clocks a, b: a **happened-before** b iff ∀i: a[i] ≤ b[i] and ∃j: a[j] < b[j]. If neither a ≤ b nor b ≤ a, they are **concurrent**.

### Quorum Intersection
For a system of N replicas with read quorum R and write quorum W, the property R + W > N guarantees that any read quorum intersects any write quorum. This intersection ensures the reader sees at least one up-to-date replica.

### Gossip Convergence
In a push-pull gossip protocol with fanout f on N nodes, the number of rounds to propagate a rumor to all nodes is O(log_f(N)) with high probability. Each round, a node exchanges state with f random peers.

### Consistent Hashing
Keys and nodes are hashed to positions on [0, 2^64). A key is assigned to the first node clockwise from its hash position. Virtual nodes (v nodes per physical node) ensure uniform distribution: the expected load imbalance is O(1/√(v)).

### Paxos Safety
Paxos guarantees that if a value v is chosen in proposal number n, then any higher-numbered proposal can only have value v. This follows from the quorum intersection property: any two quorums of ⌊N/2⌋+1 acceptors share at least one acceptor whose `Promise` forces the higher proposal to adopt v.

### Raft Log Matching
Raft guarantees that if two logs share an entry at the same index with the same term, all preceding entries are identical. The `AppendEntries` consistency check (matching `prev_log_index` and `prev_log_term`) enforces this invariant.

---

## License

MIT
