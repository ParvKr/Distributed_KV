package io.github.parvgurung.node;

import io.github.parvgurung.rpc.ClientResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** Raft should not care how many nodes there are, including even-sized clusters where majorities are less forgiving. */
class ClusterShapesTest {
    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {1, 2, 4, 6, 7})
    void anyClusterSizeElectsOneLeaderAndReplicatesWrites(int size) throws Exception {
        try (TestCluster cluster = new TestCluster(size, tempDir, 6)) {
            cluster.startAll();
            RaftNode leader = cluster.awaitLeader(20_000);
            cluster.awaitCondition("settled", 20_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));
            for (int i = 0; i < 12; i++) {
                ClientResponse r = cluster.set(cluster.awaitLeader(10_000), "k" + i, "v" + i, null, 0);
                assertTrue(r.success(), "write " + i + ": " + r.error() + "\n" + cluster.describe());
            }
            cluster.awaitCondition("convergence", 20_000, cluster::converged);
            long leaders = cluster.runningNodes().stream().filter(n -> n.termIfLeader() > 0).count();
            assertEquals(1, leaders);
            for (RaftNode n : cluster.runningNodes()) assertEquals(12, n.stateSnapshot().data().size());
            assertNotNull(leader);
        }
    }

    @Test
    void aTwoNodeClusterNeedsBothNodesToCommit() throws Exception {
        try (TestCluster cluster = new TestCluster(2, tempDir, 0)) {
            cluster.startAll();
            RaftNode leader = cluster.awaitLeader(20_000);
            cluster.awaitCondition("settled", 15_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));
            assertTrue(cluster.set(leader, "k", "1", null, 0).success());

            int other = leader.nodeId() == 1 ? 2 : 1;
            cluster.crash(other);
            assertFalse(cluster.set(leader, "while-down", "x", null, 0).success(), "one of two nodes is not a majority");

            cluster.start(other);
            RaftNode again = cluster.awaitLeader(20_000);
            cluster.awaitCondition("settled", 20_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 2));
            assertTrue(cluster.set(again, "k2", "2", null, 0).success());
            cluster.awaitCondition("convergence", 20_000, cluster::converged);
            for (RaftNode n : cluster.runningNodes()) assertEquals("1", n.stateSnapshot().data().get("k"));
        }
    }

    @Test
    void aFourNodeClusterToleratesOneFailureButNotTwo() throws Exception {
        try (TestCluster cluster = new TestCluster(4, tempDir, 0)) {
            cluster.startAll();
            RaftNode first = cluster.awaitLeader(20_000);
            cluster.awaitCondition("settled", 15_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));

            int victim1 = cluster.runningNodes().stream().filter(n -> n != first).findFirst().orElseThrow().nodeId();
            cluster.crash(victim1);
            RaftNode leader = cluster.awaitLeader(20_000);
            assertTrue(cluster.set(leader, "one-down", "ok", null, 0).success(), "3 of 4 is still a majority");

            int victim2 = cluster.runningNodes().stream().filter(n -> n != leader).findFirst().orElseThrow().nodeId();
            cluster.crash(victim2);
            RaftNode remaining = cluster.runningNodes().stream().filter(n -> n.termIfLeader() > 0).findFirst().orElse(cluster.runningNodes().get(0));
            assertFalse(cluster.set(remaining, "two-down", "no", null, 0).success(), "2 of 4 is not a majority");

            cluster.start(victim1);
            cluster.start(victim2);
            RaftNode healed = cluster.awaitLeader(25_000);
            assertTrue(cluster.set(healed, "recovered", "yes", null, 0).success());
            cluster.awaitCondition("convergence", 25_000, cluster::converged);
            for (RaftNode n : cluster.runningNodes()) {
                assertEquals("ok", n.stateSnapshot().data().get("one-down"));
                assertEquals("yes", n.stateSnapshot().data().get("recovered"));
            }
            // The refused write was never acknowledged, so it may or may not take effect once a majority returns
            // (it stays in the leader's log). What matters is that every replica reached the same verdict.
            java.util.Set<String> verdicts = new java.util.HashSet<>();
            for (RaftNode n : cluster.runningNodes()) verdicts.add(String.valueOf(n.stateSnapshot().data().get("two-down")));
            assertEquals(1, verdicts.size(), "replicas disagree about the unacknowledged write: " + verdicts);
        }
    }

    @Test
    void aSingleNodeWithSnapshotsRestartsRepeatedlyWithoutLosingAnything() throws Exception {
        try (TestCluster cluster = new TestCluster(1, tempDir, 4)) {
            cluster.startAll();
            for (int round = 0; round < 4; round++) {
                RaftNode node = cluster.awaitLeader(10_000);
                for (int i = 0; i < 9; i++) {
                    String key = "r" + round + "k" + i;
                    ClientResponse r = cluster.set(node, key, "v" + round + i, "client", round * 9L + i + 1);
                    assertTrue(r.success(), key + ": " + r.error());
                }
                cluster.crash(1);
                cluster.start(1);
            }
            RaftNode node = cluster.awaitLeader(10_000);
            cluster.awaitCondition("recovery", 10_000, () -> node.stateSnapshot().data().size() == 36);
            assertTrue(node.snapshotIndex() > 0, "snapshots should have been taken: " + node.statusSummary());
            for (int round = 0; round < 4; round++) {
                for (int i = 0; i < 9; i++) {
                    assertEquals("v" + round + i, node.stateSnapshot().data().get("r" + round + "k" + i));
                }
            }
            // the session table survived the snapshots and restarts: replaying an old request changes nothing
            assertTrue(cluster.set(node, "r0k0", "replayed", "client", 1).success());
            assertEquals("v00", node.stateSnapshot().data().get("r0k0"));
        }
    }
}
