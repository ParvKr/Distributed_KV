package io.github.parvgurung.node;

import io.github.parvgurung.rpc.ClientResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Randomised fault injection: while clients write and read, nodes are partitioned, crashed and restarted at
 * random. Whatever happens, the cluster must never break Raft's guarantees, and once the faults stop it must
 * converge and serve writes again.
 *
 * <p>Run longer with {@code -Dchaos.seconds=60}; a failure prints the seed and the fault log.
 */
class ChaosTest {
    private static final int SECONDS = Integer.getInteger("chaos.seconds", 8);
    // Shifts every seed, to explore different fault sequences: -Dchaos.seedOffset=1000
    private static final long SEED_OFFSET = Long.getLong("chaos.seedOffset", 0);

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "base seed {0}, {1} nodes")
    @CsvSource({"1,3", "2,5", "3,3", "4,5"})
    void guaranteesHoldUnderRandomPartitionsAndCrashes(long baseSeed, int size) throws Exception {
        long seed = baseSeed + SEED_OFFSET;
        Random random = new Random(seed);
        List<String> faultLog = new CopyOnWriteArrayList<>();
        List<String> violations = new CopyOnWriteArrayList<>();
        Map<String, String> acked = new ConcurrentHashMap<>();
        Map<Long, Integer> leaderOfTerm = new ConcurrentHashMap<>();
        AtomicBoolean running = new AtomicBoolean(true);

        try (TestCluster cluster = new TestCluster(size, tempDir, 8)) {
            cluster.startAll();
            cluster.awaitLeader(15_000);
            long startedAt = System.currentTimeMillis();

            // Election safety: never two different leaders in the same term.
            Thread monitor = new Thread(() -> {
                while (running.get()) {
                    for (RaftNode n : cluster.runningNodes()) {
                        long term = n.termIfLeader();
                        if (term == 0) continue;
                        Integer previous = leaderOfTerm.putIfAbsent(term, n.nodeId());
                        if (previous != null && previous != n.nodeId())
                            violations.add("two leaders in term " + term + ": nodes " + previous + " and " + n.nodeId());
                    }
                    sleep(10);
                }
            }, "chaos-monitor");

            // One writer: unique keys, so every acknowledged write must exist at the end.
            Thread writer = new Thread(() -> {
                Random r = new Random(seed * 31 + 1);
                long counter = 0;
                while (running.get()) {
                    String key = "w" + counter, value = "v" + counter;
                    try {
                        RaftNode target = cluster.leader().orElseGet(() -> pickRunning(cluster, r));
                        if (target != null) {
                            ClientResponse resp = cluster.set(target, key, value, "chaos-writer", counter + 1);
                            if (resp.success()) acked.put(key, value);
                        }
                    } catch (Exception ignored) {
                        // timeouts and refused connections are expected while faults are active
                    }
                    counter++;
                    sleep(15);
                }
            }, "chaos-writer");

            // Linearizability (the simple case): a read after an acknowledged write must see exactly that value.
            Thread reader = new Thread(() -> {
                Random r = new Random(seed * 31 + 2);
                while (running.get()) {
                    try {
                        List<String> keys = new ArrayList<>(acked.keySet());
                        RaftNode target = pickRunning(cluster, r);
                        if (!keys.isEmpty() && target != null) {
                            String key = keys.get(r.nextInt(keys.size()));
                            ClientResponse resp = cluster.get(target, key);
                            if (resp.success() && !acked.get(key).equals(resp.value()))
                                violations.add("read of acknowledged key " + key + " returned " + resp.value() + " from node " + target.nodeId()
                                    + " (expected " + acked.get(key) + ")");
                        }
                    } catch (Exception ignored) {
                    }
                    sleep(10);
                }
            }, "chaos-reader");

            monitor.start();
            writer.start();
            reader.start();

            Set<Integer> crashed = new HashSet<>();
            while (System.currentTimeMillis() - startedAt < SECONDS * 1000L) {
                int action = random.nextInt(100);
                try {
                    if (action < 25) {
                        int victim = 1 + random.nextInt(size);
                        cluster.isolate(victim);
                        faultLog.add("isolate " + victim);
                    } else if (action < 40) {
                        cluster.heal();
                        faultLog.add("heal");
                    } else if (action < 60) {
                        if (crashed.size() < (size - 1) / 2) {
                            int victim = 1 + random.nextInt(size);
                            if (cluster.isRunning(victim)) {
                                cluster.crash(victim);
                                crashed.add(victim);
                                faultLog.add("crash " + victim);
                            }
                        }
                    } else if (action < 85) {
                        if (!crashed.isEmpty()) {
                            int back = crashed.iterator().next();
                            cluster.start(back);
                            crashed.remove(back);
                            faultLog.add("restart " + back);
                        }
                    } else {
                        Optional<RaftNode> leader = cluster.leader();
                        if (leader.isPresent()) {
                            cluster.isolate(leader.get().nodeId());
                            faultLog.add("isolate leader " + leader.get().nodeId());
                        }
                    }
                } catch (Exception e) {
                    faultLog.add("fault action failed: " + e);
                }
                sleep(150 + random.nextInt(450));
            }

            // Stop the faults and let the cluster recover.
            running.set(false);
            monitor.join(5000);
            writer.join(15_000);
            reader.join(15_000);
            cluster.heal();
            for (int id : crashed) cluster.start(id);
            String context = "seed " + seed + ", " + size + " nodes, " + acked.size() + " acknowledged writes\nfaults: " + faultLog + "\n";

            assertTrue(violations.isEmpty(), context + "violations:\n" + String.join("\n", violations));

            RaftNode leader = cluster.awaitLeader(30_000);
            ClientResponse marker = null;
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                try {
                    marker = cluster.set(cluster.awaitLeader(10_000), "final-marker", "done", null, 0);
                    if (marker.success()) break;
                } catch (Exception ignored) {
                }
                Thread.sleep(100);
            }
            assertTrue(marker != null && marker.success(), context + "the healed cluster never accepted a write again\n" + cluster.describe());
            try {
                cluster.awaitCondition("convergence", 40_000, cluster::converged);
            } catch (AssertionError e) {
                throw new AssertionError(context + e.getMessage(), e);
            }

            // Durability and agreement: every acknowledged write is on every node, with the acknowledged value.
            for (RaftNode n : cluster.runningNodes()) {
                Map<String, String> data = n.stateSnapshot().data();
                for (Map.Entry<String, String> e : acked.entrySet()) {
                    assertEquals(e.getValue(), data.get(e.getKey()), context + "node " + n.nodeId() + " lost acknowledged write " + e.getKey());
                }
                assertEquals("done", data.get("final-marker"), context + "node " + n.nodeId());
            }
            Set<Long> commits = new HashSet<>();
            for (RaftNode n : cluster.runningNodes()) commits.add(n.commitIndex());
            assertEquals(1, commits.size(), context + "nodes disagree on the commit index: " + commits);
            assertNotNull(leader);
        }
    }

    private static RaftNode pickRunning(TestCluster cluster, Random random) {
        List<RaftNode> nodes = cluster.runningNodes();
        return nodes.isEmpty() ? null : nodes.get(random.nextInt(nodes.size()));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
