Page - 003

Defines the finite set of state mutations handled by the system.

[CODE LINK - statemachine/commandType.java](https://github.com/ParvKr/Distributed_KV/blob/main/distributed-kv/src/main/java/io/github/parvgurung/statemachine/CommandType.java)

#### RESPONSIBILITY

- An enumeration that defines the exact database modification primitives supported by the storage engine.
- It dictates how the Raft log entry commands will be interpreted when they are applied to the local Key-Value state machine.

#### VARIANTS

- **`SET`**: Indicates a write or update operation. Replicates a key-value pair and binds the value to the key in the storage map.
- **`DELETE`**: Indicates a removal operation. Replicates a key removal and purges the key and its mapped data from the storage map.

#### FAQs

1. Why didn't we include a `GET` variant inside this enum?
    - Because `GET` is a read-only operation and does not mutate the state machine.
    - In Raft, only operations that *modify* the system state (`SET`, `DELETE`) need to be written to the replicated log and went through full consensus execution.
    - Including `GET` in the log would cause unnecessary disk I/O and network overhead for simple reads.
2. If we want to add a new command in the future—like `APPEND` (to append text to an existing key)—how painful is it to change this architecture?
    - It is highly decoupled and scalable.
    - We would simply add `APPEND` to this enum, add the execution logic inside the  `StateMachine` implementation, and the core Raft consensus layer wouldn't care at all.
    - Raft treats the command as an opaque payload (`byte[]` or raw string); only the state machine layer interprets this enum.
