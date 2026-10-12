# I built Raft from scratch. The algorithm was the easy part.

I wanted to understand distributed consensus properly, so I built a fault-tolerant key-value store in Java using the Raft algorithm: no frameworks and no consensus libraries (the main code uses only the JDK). It runs as a cluster of nodes that agree on every write, keeps working when a minority of nodes fail, and never loses a write it has acknowledged.

Raft is famous for being understandable, and it is. The paper fits in a few pages. What the paper does not tell you is how many ways the surrounding code can go wrong. I ended up recording 24 real bugs, and most of them had nothing to do with the Raft rules.

## What I built

- Leader election with PreVote and CheckQuorum, so a node that rejoins the cluster can't knock over a healthy leader
- Log replication, with automatic repair when a follower's log has diverged
- Durable storage with crash recovery, snapshots and log compaction
- Retry-safe writes: a client can retry a timed-out request without it being applied twice
- Reads that never return stale data, even from a leader that has been cut off
- A live web dashboard served by every node, a benchmark tool, and a Docker Compose setup

## Three bugs that taught me the most

**1. A single keyword made every write fail.** My first cluster test wrote one key and got "timed out" after exactly two seconds, every time. The write handler held a lock while waiting for the commit, but the commit needed that same lock to finish. Nothing could move until the timeout released it. The write then succeeded, so clients were told it failed when it hadn't. The same stall starved the heartbeats, so followers kept starting elections; the leftover state files showed term 57. The fix was deleting the word `synchronized`.

**2. Half an emoji corrupted data.** A fuzz test generated a key containing one half of an emoji. That is valid in JSON and in Java, but UTF-8 can't encode it. The store acknowledged the write, then wrote a question mark to disk. After a restart the data was different from what had been acknowledged, and two different keys could collide. I would never have thought to write that test by hand.

**3. A web page could depose my leader.** I had locked down the client endpoints against cross-site requests, but I'd forgotten the three node-to-node endpoints. An independent code review caught it: any website could send a plain cross-site POST and make a local node adopt a forged term. Now the Raft endpoints accept only JSON requests, which forces browsers to run a safety check first.

## How I tested it

No single technique found everything. Integration tests found the concurrency bugs. Fuzzing and model-based tests found the data-layer bugs. An independent review found the security gaps. A randomized chaos test, which partitions, crashes and restarts nodes while clients write and read, checks the guarantees that matter: never two leaders in a term, no acknowledged write lost, no wrong read, and every replica converging afterwards.

The most useful lesson was about the tests themselves. I deliberately broke the voting rule to see whether the chaos test would notice. It didn't, at first. PreVote contained its own copy of the same check and was hiding the broken one. Only after I disabled both did the test fail. A test that can't fail tells you nothing.

## Performance, measured honestly

My first benchmark gave 190 writes per second. Instead of guessing, I wrote a 20-line probe: one fsync took 3.7 ms on my laptop, but 300 appends sharing one fsync took only 5.9 ms. That ratio, about fifty to one, told me where the time was going. Group commit on the leader and one in-flight request per follower brought writes to about 620 per second at 16 clients (3.3 times better) with the median latency falling from 85 ms to 25 ms. Reads reach about 4,650 per second, and a leader failover takes roughly a second.

These are laptop numbers, one machine, one JVM, loopback networking, so I treat them as a relative guide and not a production claim.

## What it can't do yet

There are no cluster membership changes, no authentication or TLS, and my tests simulate process crashes but not power loss. The write-up lists these plainly. I'd rather say what's missing than let readers find out.

## What I took away

The algorithm is the small part. The bugs lived in locks, timers, file formats, character encodings and browsers. Tests that check invariants ("one leader per term", "no acknowledged write is lost") cover scenarios nobody wrote down. And measuring before and after an optimization is worth twenty lines of code every time.

This project was built with the help of an AI coding assistant (Claude Code), including parts of the implementation, the tests and the independent code review. The design decisions, the experiments and the conclusions are what I'm sharing here.

Code, full technical report and benchmark tool: [GitHub link]

#distributedsystems #raft #java #softwareengineering
