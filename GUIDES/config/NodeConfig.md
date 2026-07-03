Responsible for the immutable, validated startup state of an individual node and calculating quorum requirements.

[config/NodeConfig.java](https://github.com/ParvKr/Distributed_KV/blob/main/distributed-kv/src/main/java/io/github/parvgurung/config/NodeConfig.java)

#### RESPONSIBILITY

Parses command-line arguments to establish the identity of the current node, sets up the local storage directory, and maps out the networking information of every other peer in the cluster.

#### FIELDS AND DATA STRUCTURES

- `public final int nodeId`: Unique identifier for this cluster node.
- `public final int port`: Port on which this node listens for incoming RPCs/client traffic.
- `public final Path dataDir`: Absolute path to where this node writes its persistent Raft log and snapshots.
- `public final Map<Integer, PeerAddress> peers`: An unmodifiable map tracking all *other* nodes in the cluster.
- `public record PeerAddress(...)`: A clean, lightweight Java record acting as a tuple for tracking a peer's identity, hostname, and port.

#### CORE METHODS

1. `clusterSize()`

- **Formula:** `peers.size() + 1`
- **Purpose:** Calculates the total count of instances in the system by taking the known remote peers and adding `1`(representing this local node).
1. `majority()`
    - **Formula:** `(clusterSize() / 2) + 1`
    - **Purpose:** Dynamically computes the strict mathematical quorum required to safely commit log entries or win a leader election.
    - *Example:* In a 3-node cluster: (3/2)+1=1+1=2. In a 5-node cluster: (5/2)+1=2+1=3.
2. `fromArgs(String[] args)`
    - **Purpose:** A factory method that safely parses custom CLI parameters (e.g., `-id=1`, `-peers=...`). It acts as a defensive guard rail, ensuring the node cannot start up in an invalid state.
    - **Key Validations Handled:**
        - Restricts ports to valid unprivileged ranges (`1024-65535`).
        - Enforces a default storage path layout (`./data/node<id>`) if `-dataDir` is omitted.
        - Throws exception if the node's own ID accidentally appears inside the`-peers` list.

#### FAQs

1. Why did we choose to use `Map.copyOf(peers)` in the constructor?
    - Defensive copying. `Map.copyOf` returns an unmodifiable map.
    - This guarantees that once `NodeConfig` is constructed, no external thread can maliciously or accidentally alter the cluster topology layout at runtime.
    - It enforces absolute immutability.
2. Why is our `majority()` method calculated dynamically instead of being hardcoded to a specific number?
    - It decouples the code from a fixed deployment topology.
    - If we scale our infrastructure from a 3-node cluster to a 5-node cluster via command-line args, the quorum boundary shifts automatically without requiring a single line of code compilation.
3. Why does our configuration validate that the node's own `nodeId` cannot exist in the `peers` map?
    - To prevent endless network loops and split-brain mathematical bugs.
    - A node must never send a `RequestVote` or `AppendEntries` RPC to itself over the network layer; it handles its own internal state locally.
4. Why did we use a Java `record` for `PeerAddress`?
    - It is the idiomatic Java choice for plain, immutable data carriers.
    - It out-of-the-box handles constructor, getters, `equals()`, `hashCode()`, and  `toString()`, keeping the codebase free of boilerplate code while remaining inherently thread-safe.
