Page - 000

### CONSENSUS

In distributed systems, **consensus** simply means getting multiple independent computers to agree on a single data value or a specific state.

Imagine five people trying to decide where to go for dinner. If they cannot talk to everyone at once, or if some people fall asleep, it becomes hard to agree. 

In a cluster of computers, consensus means all healthy servers agree on the exact same timeline of events, even if some servers crash or the network lags.

### CONSENSUS ALGORITHM

A **consensus algorithm** is the strict set of mathematical and logical rules that computers follow to reach that agreement safely.

Without these algorithms, if a server crashes mid-update, the cluster enters a **split-brain** scenario where different servers hold conflicting data. A consensus algorithm solves this by enforcing rules that guarantee reliability.

The Three Strict Rules of Consensus:

1. **Agreement**: Every healthy node must agree on the exact same value.
2. **Validity**: The agreed-upon value must have been proposed by one of the nodes (the system cannot invent a random state).
3. **Termination**: All healthy nodes must eventually reach a decision; they cannot get stuck in endless loop.

**How a Consensus Algorithm Operates:**

1. **Quorum Over Unanimity**: The algorithm does not wait for *every* server to respond. It only requires a majority (e.g., 3 out of 5 servers), known as a **quorum**. This ensures the system stays functional even if some machines fail. 
2. **Safety Over Speed**: If a quorum cannot be reached because too many machines crashed, the algorithm stops accepting writes to prevent data corruption. 
3. **Automated Recovery**: When crashed machines wake back up, the algorithm automatically feeds them missing data logs to bring them back up to speed. 

Popular Algorithms:

1. **Raft**: Designed for understandability. It elects one clear "Leader" node that dictates the state to "Follower" nodes.
2. **Paxos**: The older, mathematically rigorous grandfather of consensus. It is notoriously difficult to understand and implement correctly.
3. **Zab (ZooKeeper Atomic Broadcast)**: Used specifically by Apache ZooKeeper to manage big data cluster configurations.

### RAFT ALGORITHM

Raft breaks the complex problem of consensus into three smaller, independent sub-problems: **Leader Election**, **Log Replication**, and **Safety.**

At any given time, every server in a Raft cluster is always in one of three states:

- **Leader**: Handles all client requests and manages log replication. (Only 1 active leader exists).
- **Follower**: Completely passive. They respond to requests from leaders and candidates but never initiate communication.
- **Candidate**: A follower that timed out waiting for a leader and is actively trying to get elected

1. Leader Election (Who is in charge?)

**The Heartbeat**: The Leader constantly sends empty messages called `AppendEntries`  (heartbeats) to all Followers. This tells them, "I am still alive, do not start an election."

**Election Timeout**: Every Follower has a countdown clock called an **Election Timeout** (typically a random duration between 150ms and 300ms). Every time a follower receives a heartbeat, its clock resets to zero.

**Starting an Election**: If a Follower’s clock runs out because the Leader crashed or the network dropped, the Follower changes its state to **Candidate**. It increments the cluster's **Term** (a term is like an election year/counter) and votes for itself.

**Voting**: The Candidate sends a `RequestVote` message to all other nodes.

- If a node hasn't voted in this term yet, and the Candidate's log is at least as up-to-date as its own, it votes "Yes".
- If the Candidate wins a majority (a **quorum**, e.g., 3 out of 5 nodes), it instantly becomes the new **Leader** and starts blasting heartbeats to stop other nodes from timing out

**Split Votes**: If two followers time out at the exact same time, they might split the votes evenly (e.g., 2 votes each in a 5-node cluster). Because of the **randomized timeouts**, one node will naturally time out faster than the other in the next round, break the tie, and win the election

1. **Log Replication (How data is saved)**
    
    Once a Leader is elected, it manages all modifications to the system state via an ordered append-only log.
    
    **Receive Command**: A client sends a write request (e.g., `SET x=10`) to the Leader. 
    
    **Append Locally**: The Leader writes this command to its own local log entry. It is **not yet committed** or applied to the actual database map.
    
    **Broadcast**: The Leader sends `AppendEntries` RPCs containing this new log entry to all Followers.
    
    **Follower Write**: Followers receive the log entry, write it to their own local disks, and reply back to the Leader confirming the write. 
    
    **Commit Phase**: Once the Leader receives confirmations from a **majority** of nodes, the entry is officially **Committed**. The Leader applies `x=10` to its internal state machine (the final database map) and sends a success response to the client. 
    
    **Follower Sync**: On the next heartbeat, the Leader notifies the followers that the entry is committed. The followers then safely apply the change to their own internal state machines.
    
2. **Raft Safety Guarantees (Why data is never lost)**
    
    Distributed systems crash and lose network connection all the time. Raft enforces strict safety rules to guarantee that once a log entry is "committed", it can never be overwritten or lost.
    
    **Election Safety**: Only **one** leader can be elected per Term. This prevents two computers from writing conflicting data simultaneously.
    
    **Leader Append-Only**: A Leader never overwrites or deletes its own log entries. It only appends new ones.
    
    **Log Matching Property**: If two separate logs contain an entry with the exact same index and term, then they are guaranteed to be identical up to that index.
    
    **Leader Completeness (Crucial Rule)**: If a log entry is committed in a given term, that entry **must** be present in the logs of the leaders for all higher-numbered terms.
        ◦ *How?* Followers will reject a `RequestVote` if the candidate's log is less complete than their own. Since a committed entry must reside on a majority of nodes, any winning candidate *must* cross paths with at least one node holding that committed entry, preventing outdated nodes from ever becoming leader.
    

### RAFT ALGORITHM - ANALYSIS

**Scenario 1: Leader Crashes Mid-Write (Uncommitted Log Cleanup)**

Imagine a 5-node cluster (Nodes A, B, C, D, E). Node A is the Leader in **Term 1.**

1.  **The Interruption -** A client sends a command: `SET "score" = 100`.
    1. Node A writes this to its local log at **Index 5** and sends it to Node B. 
    2. Before Nodes C, D, or E receive it, **Node A suddenly loses power and crashes.**
2. **The Current State** 
    1. **Node A (Crashed)**: Has `score = 100` at Index 5. (Uncommitted)
    2. **Node B**: Has `score = 100` at Index 5. (Uncommitted)
    3. **Nodes C, D, E**: Their logs stop at Index 4. They know nothing about `score = 100`.
3. **The Recovery -** Because Node A stopped sending heartbeats, Nodes C, D, and E time out. Node C initiates an election for **Term 2**.
    1. Node C asks B, D, and E for votes.
    2. Nodes D and E vote "Yes" because their logs match C's log perfectly.
    3. Node C wins the majority (3 out of 5 votes) and becomes the **New Leader for Term 2**.
4. **Handling the Conflict -** A client sends a new write to the new leader, Node C: `SET "score" = 50`.Node C appends this to its local log at **Index 5** (overwriting nothing, because its index 5 was empty) and replicates it to the cluster.
    1. When Node C sends this to **Node B**, Node B notices a conflict: "My Index 5 says `Term 1 / score=100`, but the Leader says `Term 2 / score=50`."
    2. **Raft Rule**: The Leader's log is the absolute source of truth. Node B must **overwrite** its uncommitted Term 1 entry with the Leader’s Term 2 entry.
    3. Once a majority accepts `score = 50`, it is committed. If Node A eventually wakes back up, it will also be forced to overwrite its old, uncommitted entry to match Node C.

**Scenario 2: Network Partition (The Split-Brain Test)**

This is the ultimate test for a distributed system. 

Imagine a 5-node cluster spread across two data centers. A backhoe cuts the network cable between them, splitting the cluster into two isolated isolated islands.

$$
Node A (leader), Node B	----XXX---Node C, D, E
$$

1.  **Behavior in Partition 1 (The Minority)**
    
    Node A still thinks it is the leader. A client connected to the West data center tries to write `SET x = 1`.
    
    1. Node A accepts the write and appends it to its log.
    2. Node A sends it to Node B. Node B accepts it.
    3. Node A tries to send it to C, D, and E, but the network is dead.
    4. Node A only has **2 out of 5** confirmations. This is *not* a majority. **The entry remains uncommitted.** The client's write hangs or receives an error.
2. **Behavior in Partition 2 (The Majority)**
    
    Nodes C, D, and E stop receiving heartbeats from Node A. They time out and trigger an election.
    
    1. Because they have 3 nodes, they can successfully form a **Quorum (3 out of 5)**.
    2. They elect **Node C** as the new Leader for a higher Term.
    3. A client connected to the East data center writes `SET x = 2`.
    4. Node C replicates this to D and E. It gets **3 out of 5** confirmations. **This entry is successfully Committed!**
3. **The Heal (Reconnecting the Cluster)**
    
    The network cable is repaired, and all 5 nodes can talk again.
    
    1. Old Leader Node A tries to send a heartbeat to Node C.
    2. Node C rejects it and says, "Your term is old. I am the Leader of a newer Term."
    3. Node A immediately realizes it is out of date, **steps down**, and reverts to a Follower state.
    4. Node C looks at Node B and Node A's logs, spots the uncommitted `x = 1` from the partition era, and forces them to overwrite their logs with the committed `x = 2`.

The cluster is perfectly unified again, and no committed data was lost!

### RAFT ALGORITHM - OPTIMIZATIONS

1. Log Compaction (Snapshots)
    
    If a system runs millions of operations a day, an append-only log will eventually fill up the hard drive and take hours to replay when a server restarts. Raft solves this using **Snapshotting.**
    
    **The Concept**: Instead of keeping a log of how a value *became* what it is, a node saves the **current final state** to disk and discards the historical logs.
    
    **The Boundary**: The snapshot must record the **Last Included Index** and **Last Included Term** of the highest log entry it replaced. This allows remaining logs to stitch perfectly onto the end of the snapshot.
    
    **Catching Up Slow Followers**: If a follower has been offline for a week, its log is too far behind. The Leader cannot send incremental logs because they were deleted. Instead, the Leader uses a special RPC called `InstallSnapshot` to send its entire snapshot file to the follower at once.
    
2. **Cluster Membership Changes (Adding/Removing Nodes)**
    
    You cannot simply update a configuration file on all servers to add a 6th and 7th node. Because servers restart at different times, some might think a majority is 3 out of 5, while others think it is 4 out of 7. This creates a temporary window where **two independent majorities** can form, causing a split-brain. 
    
    To prevent this, Raft uses a two-phase protocol called **Joint Consensus**:
    1. **Joint State $(C_{old,new})$**: The Leader proposes a configuration change log entry containing *both*the old and new layouts.
    2. **Double Quorum Rule**: While in this joint state, any decision (like committing a log or electing a leader) requires **two independent majorities**: a majority of the old cluster layout *and* a majority of the new cluster layout.
    3. **Final Transition $(C_{new})$**: Once the joint configuration log entry is committed across both majorities, the Leader broadcasts a final log entry activating the new configuration  **$(C_{new})$.** The old configuration is safely dropped.
    
3. **Linearizable Reads (Preventing Stale Data)**
    
    By default, Raft handles writes safely via log majorities. However, if a client reads data (`GET x`) from a Leader that was just network-partitioned, that Leader might serve old data because it doesn't know it has been deposed yet.
    
    To guarantee **Linearizability** (ensuring a read always returns the latest committed write), Raft implementations use one of two strategies:
    
    1. **Read Index (The Safe Standard)**: When a Leader receives a read request, it records its current commit index. Before replying, it must send a quick round of empty heartbeat RPCs to the cluster. If a majority responds, the Leader knows it is still the legitimate leader and can safely serve the data.
    2. **Leases (The High-Performance Standard)**: The cluster agrees that once a Leader is elected, it holds a time-bounded "lease" (e.g., 500ms) during which no other leader can possibly be elected. The Leader can answer reads instantly without network round-trips, provided its local clock says the lease is active.

[START - \[001\]config/NodeConfig.md](https://github.com/ParvKr/Distributed_KV/blob/main/GUIDES/config/NodeConfig.md)
