package io.github.parvgurung.node;

import io.github.parvgurung.rpc.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Raft's safety properties, exercised through partitions, crashes and restarts. */
class SafetyScenarioTest {
    @TempDir
    Path tempDir;

    private TestCluster cluster;

    @AfterEach
    void stop() throws Exception {
        if (cluster != null) cluster.close();
    }

    private TestCluster start(int size, int snapshotThreshold) throws Exception {
        cluster = new TestCluster(size, tempDir, snapshotThreshold);
        cluster.startAll();
        return cluster;
    }

    /** Waits until a leader exists and every node has seen its no-op, i.e. the cluster is settled. */
    private RaftNode settledLeader() throws Exception {
        cluster.awaitCondition("all nodes to commit the first no-op", 15_000,
            () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));
        return cluster.awaitLeader(10_000);
    }

    private void assertEveryNodeHas(String key, String expected) {
        for (RaftNode n : cluster.runningNodes()) {
            assertEquals(expected, n.stateSnapshot().data().get(key), "node " + n.nodeId() + " key " + key + "\n" + cluster.describe());
        }
    }

    private void assertNoNodeHas(String key) {
        for (RaftNode n : cluster.runningNodes()) {
            assertFalse(n.stateSnapshot().data().containsKey(key), "node " + n.nodeId() + " has " + key + "\n" + cluster.describe());
        }
    }

    // ------------------------------------------------------------------ writes that were never committed

    @Test
    void aWriteTakenByACutOffLeaderIsNeverCommittedOnceTheClusterMovesOn() throws Exception {
        start(3, 0);
        RaftNode oldLeader = settledLeader();
        assertTrue(cluster.set(oldLeader, "before", "1", null, 0).success());

        cluster.isolate(oldLeader.nodeId());
        ClientResponse ghost = cluster.set(oldLeader, "ghost", "boo", null, 0);
        assertFalse(ghost.success(), "a write with no majority must not be acknowledged");

        RaftNode newLeader = cluster.awaitLeaderOtherThan(15_000, oldLeader.nodeId());
        assertTrue(cluster.set(newLeader, "real", "2", null, 0).success());

        cluster.heal();
        cluster.awaitCondition("convergence", 20_000, cluster::converged);
        assertNoNodeHas("ghost");
        assertEveryNodeHas("before", "1");
        assertEveryNodeHas("real", "2");
    }

    @Test
    void theCutOffLeaderCanCrashAndRestartAndItsUncommittedTailIsStillRepaired() throws Exception {
        start(3, 0);
        RaftNode oldLeader = settledLeader();
        int oldId = oldLeader.nodeId();
        cluster.isolate(oldId);
        for (int i = 0; i < 3; i++) assertFalse(cluster.set(oldLeader, "ghost" + i, "x", null, 0).success());
        RaftNode newLeader = cluster.awaitLeaderOtherThan(15_000, oldId);
        assertTrue(cluster.set(newLeader, "real", "2", null, 0).success());

        cluster.crash(oldId);   // the stale entries are on its disk now
        cluster.heal();
        cluster.start(oldId);

        cluster.awaitCondition("convergence", 20_000, cluster::converged);
        for (int i = 0; i < 3; i++) assertNoNodeHas("ghost" + i);
        assertEveryNodeHas("real", "2");
    }

    @Test
    void aMinorityOfAFiveNodeClusterCannotCommitAndTheMajorityCan() throws Exception {
        start(5, 0);
        RaftNode oldLeader = settledLeader();
        RaftNode otherFollower = cluster.runningNodes().stream().filter(n -> n != oldLeader).findFirst().orElseThrow();
        cluster.isolate(oldLeader.nodeId());
        cluster.isolate(otherFollower.nodeId());

        assertFalse(cluster.set(oldLeader, "minority", "no", null, 0).success());
        RaftNode newLeader = cluster.awaitLeaderOtherThan(15_000, oldLeader.nodeId(), otherFollower.nodeId());
        assertTrue(cluster.set(newLeader, "majority", "yes", null, 0).success());

        cluster.heal();
        cluster.awaitCondition("convergence", 25_000, cluster::converged);
        assertNoNodeHas("minority");
        assertEveryNodeHas("majority", "yes");
    }

    // ------------------------------------------------------------------ reads

    @Test
    void readsNeverReturnStaleDataFromADeposedLeader() throws Exception {
        start(3, 0);
        RaftNode oldLeader = settledLeader();
        assertTrue(cluster.set(oldLeader, "k", "1", null, 0).success());

        cluster.isolate(oldLeader.nodeId());
        RaftNode newLeader = cluster.awaitLeaderOtherThan(15_000, oldLeader.nodeId());
        assertTrue(cluster.set(newLeader, "k", "2", null, 0).success());

        // While cut off, the old leader may still think it leads for a moment; it must never answer "1".
        for (int i = 0; i < 10; i++) {
            ClientResponse stale = cluster.get(oldLeader, "k");
            assertFalse(stale.success() && "1".equals(stale.value()), "stale read served by a deposed leader");
            Thread.sleep(100);
        }
        cluster.awaitCondition("the new leader to serve the latest value", 10_000, () -> {
            try {
                ClientResponse r = cluster.get(newLeader, "k");
                return r.success() && "2".equals(r.value());
            } catch (Exception e) {
                return false;
            }
        });

        cluster.heal();
        cluster.awaitCondition("convergence", 20_000, cluster::converged);
        for (RaftNode n : cluster.runningNodes()) {
            ClientResponse r = cluster.get(n, "k");
            assertFalse(r.success() && !"2".equals(r.value()), "node " + n.nodeId() + " served " + r.value());
        }
    }

    // ------------------------------------------------------------------ request IDs across failures

    @Test
    void aRetriedWriteIsStillRecognisedAfterALeaderFailover() throws Exception {
        start(3, 0);
        RaftNode oldLeader = settledLeader();
        assertTrue(cluster.set(oldLeader, "k", "from-A", "A", 1).success());
        assertTrue(cluster.set(oldLeader, "k", "from-B", "B", 1).success());
        cluster.awaitCondition("replication", 10_000, cluster::converged);

        cluster.crash(oldLeader.nodeId());
        RaftNode newLeader = cluster.awaitLeaderOtherThan(15_000, oldLeader.nodeId());

        // A never saw the reply and retries against the new leader.
        ClientResponse retry = cluster.set(newLeader, "k", "from-A", "A", 1);
        assertTrue(retry.success(), retry.error());
        cluster.awaitCondition("convergence", 10_000, cluster::converged);
        assertEveryNodeHas("k", "from-B");
    }

    // ------------------------------------------------------------------ concurrency

    @Test
    void manyConcurrentClientsAllGetTheirWritesAppliedExactlyOnceEverywhere() throws Exception {
        start(3, 10);
        RaftNode leader = settledLeader();
        int clients = 6, perClient = 25;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<List<String>>> results = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            final int id = c;
            results.add(pool.submit(() -> {
                List<String> acked = new ArrayList<>();
                for (int i = 0; i < perClient; i++) {
                    String key = "c" + id + "-k" + i;
                    ClientResponse r = cluster.set(cluster.awaitLeader(10_000), key, "v" + i, "client-" + id, i + 1L);
                    if (r.success()) acked.add(key);
                }
                return acked;
            }));
        }
        List<String> allAcked = new ArrayList<>();
        for (Future<List<String>> f : results) allAcked.addAll(f.get(120, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals(clients * perClient, allAcked.size(), "every write should be acknowledged on a healthy cluster");
        cluster.awaitCondition("convergence", 20_000, cluster::converged);
        for (String key : allAcked) {
            String expected = "v" + key.substring(key.indexOf("-k") + 2);
            assertEveryNode(key, expected);
        }
        for (RaftNode n : cluster.runningNodes()) assertEquals(clients * perClient, n.stateSnapshot().data().size());
    }

    private void assertEveryNode(String key, String expected) {
        assertEveryNodeHas(key, expected);
    }

    @Test
    void contendedWritesToOneKeyLeaveEveryReplicaWithTheSameValue() throws Exception {
        start(3, 8);
        settledLeader();
        int writers = 5;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        Set<String> written = ConcurrentHashMap.newKeySet();
        List<Future<?>> done = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int id = w;
            done.add(pool.submit(() -> {
                for (int i = 0; i < 20; i++) {
                    String value = "w" + id + "-" + i;
                    try {
                        ClientResponse r = cluster.set(cluster.awaitLeader(10_000), "hot", value, "writer-" + id, i + 1L);
                        if (r.success()) written.add(value);
                    } catch (Exception ignored) {
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : done) f.get(120, TimeUnit.SECONDS);
        pool.shutdownNow();

        cluster.awaitCondition("convergence", 20_000, cluster::converged);
        Set<String> finalValues = new HashSet<>();
        for (RaftNode n : cluster.runningNodes()) finalValues.add(n.stateSnapshot().data().get("hot"));
        assertEquals(1, finalValues.size(), "replicas disagree: " + finalValues);
        assertTrue(written.contains(finalValues.iterator().next()), "the final value was never written by anyone");
    }

    // ------------------------------------------------------------------ odd data end to end

    @Test
    void awkwardKeysAndValuesSurviveReplicationSnapshotsAndAFullRestart() throws Exception {
        start(3, 5);
        RaftNode leader = settledLeader();
        Map<String, String> data = new LinkedHashMap<>();
        data.put("pipe|key", "a | b | SET|x|y");
        data.put("new\nline", "line1\nline2\r\nline3");
        data.put("quote\"s", "it's \"quoted\"");
        data.put("back\\slash", "\\\\ \\p \\n \\");
        data.put("日本語", "こんにちは");
        data.put("emoji😀key", "🚀🚀");
        data.put("  spaces  ", "   ");
        data.put("tab\tkey", "\t");
        data.put("carriage\rreturn", "\r");
        data.put("percent%20key", "100%");
        data.put("plus+key", "a+b c");
        data.put("line sep", "para sep");
        data.put("empty-value", "");
        data.put("long", "z".repeat(100_000));
        data.put("json{}[]:,", "{\"nested\":[1,2,{\"x\":null}]}");
        for (Map.Entry<String, String> e : data.entrySet()) {
            ClientResponse r = cluster.set(leader, e.getKey(), e.getValue(), null, 0);
            assertTrue(r.success(), "set " + e.getKey().replace("\n", "\\n") + ": " + r.error());
        }
        for (int i = 0; i < 20; i++) assertTrue(cluster.set(leader, "filler" + i, "x", null, 0).success()); // force snapshots
        cluster.awaitCondition("snapshots", 15_000, () -> cluster.runningNodes().stream().allMatch(n -> n.snapshotIndex() > 0));
        cluster.awaitCondition("convergence", 15_000, cluster::converged);
        for (Map.Entry<String, String> e : data.entrySet()) assertEveryNodeHas(e.getKey(), e.getValue());

        for (RaftNode n : cluster.runningNodes()) cluster.crash(n.nodeId());
        for (int id = 1; id <= cluster.size; id++) cluster.start(id);
        cluster.awaitLeader(15_000);
        cluster.awaitCondition("recovery", 25_000, () -> cluster.runningNodes().stream().allMatch(n -> n.stateSnapshot().data().size() >= data.size() + 20) && cluster.converged());
        for (Map.Entry<String, String> e : data.entrySet()) assertEveryNodeHas(e.getKey(), e.getValue());

        RaftNode again = cluster.awaitLeader(10_000);
        for (Map.Entry<String, String> e : data.entrySet()) {
            ClientResponse r = awaitRead(again, e.getKey());
            assertEquals(e.getValue(), r.value(), "read of " + e.getKey().replace("\n", "\\n"));
        }
    }

    private ClientResponse awaitRead(RaftNode leader, String key) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        ClientResponse last = null;
        while (System.currentTimeMillis() < deadline) {
            last = cluster.get(cluster.awaitLeader(5_000), key);
            if (last.success()) return last;
            Thread.sleep(50);
        }
        return fail("read never succeeded: " + last);
    }

    @Test
    void aRestartedFollowerRepeatedlyBouncedDuringWritesAlwaysCatchesUp() throws Exception {
        start(3, 6);
        RaftNode leader = settledLeader();
        int bouncing = cluster.runningNodes().stream().filter(n -> n != leader).findFirst().orElseThrow().nodeId();
        Set<String> acked = new HashSet<>();
        for (int round = 0; round < 4; round++) {
            cluster.crash(bouncing);
            for (int i = 0; i < 10; i++) {
                String key = "r" + round + "-" + i;
                if (cluster.set(cluster.awaitLeader(10_000), key, "v", null, 0).success()) acked.add(key);
            }
            cluster.start(bouncing);
        }
        cluster.awaitCondition("convergence", 25_000, cluster::converged);
        for (String key : acked) assertEveryNodeHas(key, "v");
        assertEquals(40, acked.size());
    }
}
