package io.github.parvgurung.node;

import io.github.parvgurung.client.KVClient;
import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.log.PersistentLog;
import io.github.parvgurung.rpc.ClientResponse;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.util.Json;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class RaftClusterTest {
    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final Map<Integer, Integer> portById = new HashMap<>();
    private final Map<Integer, NodeConfig> configById = new HashMap<>();
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stopCluster() throws Exception {
        for (RaftNode node : nodes) node.shutdown();
    }

    // ---------------------------------------------------------------- election

    @Test
    void electsExactlyOneLeader() throws Exception {
        startCluster(3);
        awaitLeader();
        assertEquals(1, nodes.stream().filter(n -> n.role() == NodeRole.LEADER).count());
    }

    @Test
    void newLeaderCommitsANoopInItsOwnTerm() throws Exception {
        startCluster(3);
        awaitLeader();
        awaitAllNodesCommitted(1);
    }

    // ---------------------------------------------------------------- writes

    @Test
    void leaderSetSucceedsPromptlyAndReplicatesToAllNodes() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();

        long start = System.nanoTime();
        ClientResponse set = post(leader, "/client/set", "key", "k1", "value", "v1");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(set.success(), "write failed: " + set.error());
        assertTrue(elapsedMs < 1000, "write took " + elapsedMs + "ms; replication should not be blocked by the handler");
        assertEquals("v1", awaitGet(leader, "k1"));
        awaitAllNodesCommitted(2); // no-op + the write
    }

    @Test
    void leaderStaysStableWhileServingWrites() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        long termBefore = termOf(leader);

        for (int i = 0; i < 5; i++) {
            ClientResponse r = post(leader, "/client/set", "key", "k" + i, "value", "v" + i);
            assertTrue(r.success(), "write " + i + " failed: " + r.error());
        }

        assertEquals(NodeRole.LEADER, leader.role());
        assertEquals(termBefore, termOf(leader), "writes must not trigger new elections");
    }

    @Test
    void deleteRemovesKey() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        ClientResponse set = post(leader, "/client/set", "key", "k", "value", "v");
        assertTrue(set.success(), "set failed: " + set.error());
        ClientResponse del = post(leader, "/client/delete", "key", "k");
        assertTrue(del.success(), "delete failed: " + del.error());
        assertNull(awaitGet(leader, "k"));
    }

    @Test
    void followerRedirectsToLeader() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        awaitAllNodesKnowLeader();
        RaftNode follower = nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

        ClientResponse r = post(follower, "/client/set", "key", "k", "value", "v");

        assertFalse(r.success());
        assertEquals("localhost:" + portById.get(leader.nodeId()), r.leaderHint());
    }

    @Test
    void singleNodeClusterCommitsWrites() throws Exception {
        startCluster(1);
        RaftNode leader = awaitLeader();

        ClientResponse set = post(leader, "/client/set", "key", "k", "value", "v");
        assertTrue(set.success(), "write failed: " + set.error());
        assertEquals("v", awaitGet(leader, "k"));
    }

    @Test
    void invalidWriteIsRejectedAndDoesNotPoisonTheLog() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();

        ClientResponse bad = post(leader, "/client/set", "key", "k"); // no value
        assertFalse(bad.success());
        assertNotNull(bad.error());

        ClientResponse good = post(leader, "/client/set", "key", "k", "value", "v");
        assertTrue(good.success(), "valid write failed: " + good.error());
        assertEquals("v", awaitGet(leader, "k"));
    }

    // ---------------------------------------------------------------- failure handling

    @Test
    void dataSurvivesLeaderFailover() throws Exception {
        startCluster(3);
        RaftNode oldLeader = awaitLeader();
        assertTrue(post(oldLeader, "/client/set", "key", "k", "value", "v").success());
        awaitAllNodesCommitted(2);

        oldLeader.shutdown();
        RaftNode newLeader = awaitLeaderOtherThan(oldLeader);

        assertEquals("v", awaitGet(newLeader, "k"));
        assertTrue(post(newLeader, "/client/set", "key", "k2", "value", "v2").success());
    }

    @Test
    void isolatedLeaderRefusesToServeReads() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        assertTrue(post(leader, "/client/set", "key", "k", "value", "v").success());

        for (RaftNode n : nodes) {
            if (n != leader) n.shutdown();
        }

        // The old leader still thinks it leads, but can no longer prove it, so it must not answer.
        ClientResponse read = get(leader, "k");
        assertFalse(read.success(), "read served without a majority confirming leadership");
    }

    @Test
    void divergentFollowerLogIsRepairedByTheLeader() throws Exception {
        // Nodes 1 and 3 share a log whose entry 2 is from term 2. Node 2 holds stale term-1 entries that
        // were never committed. Node 2 cannot win the first election, so the leader must repair its log.
        seed(1, 2, new LogEntry(1, 1, Command.set("a", "1")), new LogEntry(2, 2, Command.set("b", "2")));
        seed(3, 2, new LogEntry(1, 1, Command.set("a", "1")), new LogEntry(2, 2, Command.set("b", "2")));
        seed(2, 1, new LogEntry(1, 1, Command.set("a", "1")), new LogEntry(2, 1, Command.set("stale", "x")),
            new LogEntry(3, 1, Command.set("stale2", "x")));

        startCluster(3);
        RaftNode leader = awaitLeader();

        awaitAllNodesCommitted(3); // entries 1-2 plus the leader's no-op at index 3
        assertEquals("2", awaitGet(leader, "b"));
        assertNull(awaitGet(leader, "stale"));

        String repaired = Files.readString(tempDir.resolve("node2").resolve("raft.log"));
        assertFalse(repaired.contains("stale"), "node 2 kept its divergent entries:\n" + repaired);
    }

    @Test
    void dataSurvivesFullClusterRestart() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        assertTrue(post(leader, "/client/set", "key", "durable", "value", "yes").success());
        awaitAllNodesCommitted(2);

        for (RaftNode n : nodes) n.shutdown();
        nodes.clear();
        startCluster(3); // same data dirs and ports

        RaftNode newLeader = awaitLeader();
        assertEquals("yes", awaitGet(newLeader, "durable"));
        assertTrue(post(newLeader, "/client/set", "key", "after", "value", "restart").success());
    }

    @Test
    void clientRidesThroughLeaderFailover() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        KVClient client = new KVClient(nodes.stream().map(n -> "localhost:" + portById.get(n.nodeId())).toList());
        client.set("before", "1");

        leader.shutdown(); // the client must wait out the election instead of giving up
        client.set("after", "2");

        assertEquals("1", client.get("before"));
        assertEquals("2", client.get("after"));
    }

    @Test
    void retriedWriteDoesNotOverwriteALaterWriteFromAnotherClient() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();

        assertTrue(post(leader, "/client/set", "key", "k", "value", "from-A", "clientId", "A", "seq", 1).success());
        assertTrue(post(leader, "/client/set", "key", "k", "value", "from-B", "clientId", "B", "seq", 1).success());
        // A never saw the first reply and retries the very same request.
        ClientResponse retry = post(leader, "/client/set", "key", "k", "value", "from-A", "clientId", "A", "seq", 1);

        assertTrue(retry.success(), "a retry of an applied write should be acknowledged: " + retry.error());
        assertEquals("from-B", awaitGet(leader, "k"));
    }

    @Test
    void requestDedupSurvivesFullClusterRestart() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        assertTrue(post(leader, "/client/set", "key", "k", "value", "from-A", "clientId", "A", "seq", 1).success());
        assertTrue(post(leader, "/client/set", "key", "k", "value", "from-B", "clientId", "B", "seq", 1).success());
        awaitAllNodesCommitted(3);

        for (RaftNode n : nodes) n.shutdown();
        nodes.clear();
        startCluster(3);

        RaftNode newLeader = awaitLeader();
        assertEquals("from-B", awaitGet(newLeader, "k")); // log replayed; sessions rebuilt with it
        ClientResponse retry = post(newLeader, "/client/set", "key", "k", "value", "from-A", "clientId", "A", "seq", 1);
        assertTrue(retry.success(), retry.error());
        assertEquals("from-B", awaitGet(newLeader, "k"));
    }

    @Test
    void sessionWithoutSequenceIsABadRequest() throws Exception {
        startCluster(1);
        RaftNode leader = awaitLeader();
        ClientResponse r = post(leader, "/client/set", "key", "k", "value", "v", "clientId", "A");
        assertFalse(r.success());
    }

    @Test
    void statusJsonDescribesTheNode() throws Exception {
        startCluster(3, 5);
        RaftNode leader = awaitLeader();
        for (int i = 0; i < 8; i++) assertTrue(post(leader, "/client/set", "key", "k" + i, "value", "v").success());
        awaitCondition("leader to snapshot", () -> leader.snapshotIndex() > 0);

        HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri(leader, "/status.json")).GET().build(),
            HttpResponse.BodyHandlers.ofString());

        assertEquals(200, r.statusCode());
        assertEquals("*", r.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
        Map<String, Object> status = Json.decode(r.body());
        assertEquals("LEADER", status.get("role"));
        assertEquals(leader.nodeId(), Json.getInt(status, "nodeId"));
        assertEquals(leader.nodeId(), Json.getInt(status, "leaderId"));
        assertEquals(8, Json.getInt(status, "keys"));
        assertTrue(Json.getLong(status, "snapshotIndex") > 0);
        assertEquals(5, Json.getInt(status, "snapshotThreshold"));
        assertTrue(Json.getLong(status, "commitIndex") >= Json.getLong(status, "snapshotIndex"));
    }

    @Test
    void getWithoutKeyIsABadRequest() throws Exception {
        startCluster(3);
        RaftNode leader = awaitLeader();
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri(leader, "/client/get")).GET().build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(400, r.statusCode());
        assertFalse(ClientResponse.fromJson(r.body()).success());
    }

    @Test
    void clientDoesNotRetryABadRequest() throws Exception {
        startCluster(1);
        awaitLeader();
        KVClient client = new KVClient(List.of("localhost:" + portById.get(1)));
        assertThrows(IllegalArgumentException.class, () -> client.set("", "v"));
    }

    // ---------------------------------------------------------------- snapshots

    @Test
    void logIsCompactedOnEveryNodeAndNoDataIsLost() throws Exception {
        startCluster(3, 5);
        RaftNode leader = awaitLeader();

        for (int i = 0; i < 30; i++) {
            ClientResponse r = post(leader, "/client/set", "key", "k" + i, "value", "v" + i);
            assertTrue(r.success(), "write " + i + " failed: " + r.error());
        }

        awaitCondition("every node to compact its log", () -> nodes.stream().allMatch(n -> n.snapshotIndex() >= 20));
        for (int i = 0; i < 30; i++) {
            assertEquals("v" + i, awaitGet(leader, "k" + i));
        }
        for (RaftNode n : nodes) {
            assertTrue(Files.exists(tempDir.resolve("node" + n.nodeId()).resolve("snapshot")));
            long keptInLog = Files.readAllLines(tempDir.resolve("node" + n.nodeId()).resolve("raft.log")).size();
            assertTrue(keptInLog < 15, "node " + n.nodeId() + " still holds " + keptInLog + " entries on disk");
        }
    }

    @Test
    void clusterRestartsFromSnapshotPlusLogTail() throws Exception {
        startCluster(3, 5);
        RaftNode leader = awaitLeader();
        assertTrue(post(leader, "/client/set", "key", "contested", "value", "from-A", "clientId", "A", "seq", 1).success());
        assertTrue(post(leader, "/client/set", "key", "contested", "value", "from-B", "clientId", "B", "seq", 1).success());
        for (int i = 0; i < 20; i++) {
            assertTrue(post(leader, "/client/set", "key", "k" + i, "value", "v" + i).success());
        }
        awaitCondition("every node to snapshot", () -> nodes.stream().allMatch(n -> n.snapshotIndex() >= 10));
        long lastIndex = leader.lastLogIndex();
        awaitAllNodesCommitted(lastIndex);

        for (RaftNode n : nodes) n.shutdown();
        nodes.clear();
        startCluster(3, 5);

        RaftNode newLeader = awaitLeader();
        for (int i = 0; i < 20; i++) {
            assertEquals("v" + i, awaitGet(newLeader, "k" + i));
        }
        assertEquals("from-B", awaitGet(newLeader, "contested"));
        // A's retry must still be recognised as a duplicate: that knowledge lives in the snapshot now.
        assertTrue(post(newLeader, "/client/set", "key", "contested", "value", "from-A", "clientId", "A", "seq", 1).success());
        assertEquals("from-B", awaitGet(newLeader, "contested"));
    }

    @Test
    void followerThatFellBehindTheCompactedLogCatchesUpViaSnapshot() throws Exception {
        startCluster(3, 5);
        RaftNode leader = awaitLeader();
        RaftNode lagging = nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
        assertTrue(post(leader, "/client/set", "key", "early", "value", "e").success());
        awaitAllNodesCommitted(2);

        lagging.shutdown();
        for (int i = 0; i < 30; i++) {
            ClientResponse r = post(leader, "/client/set", "key", "k" + i, "value", "v" + i);
            assertTrue(r.success(), "write " + i + " failed: " + r.error());
        }
        // The leader has now compacted past everything the follower has.
        awaitCondition("leader to compact beyond the lagging follower", () -> leader.snapshotIndex() > 5);

        RaftNode revived = new RaftNode(configById.get(lagging.nodeId()));
        nodes.set(nodes.indexOf(lagging), revived);
        revived.start();

        awaitCondition("revived follower to catch up", () -> revived.commitIndex() >= leader.commitIndex() && revived.snapshotIndex() > 0);
        assertTrue(revived.statusSummary().contains("entries=31"), revived.statusSummary());
        assertEquals(leader.lastLogIndex(), revived.lastLogIndex());
    }

    @Test
    void snapshotsKeepWorkingThroughLeaderFailover() throws Exception {
        startCluster(3, 5);
        RaftNode oldLeader = awaitLeader();
        for (int i = 0; i < 20; i++) {
            assertTrue(post(oldLeader, "/client/set", "key", "k" + i, "value", "v" + i).success());
        }
        awaitCondition("every node to snapshot", () -> nodes.stream().allMatch(n -> n.snapshotIndex() >= 10));

        oldLeader.shutdown();
        RaftNode newLeader = awaitLeaderOtherThan(oldLeader);

        for (int i = 0; i < 20; i++) {
            assertEquals("v" + i, awaitGet(newLeader, "k" + i));
        }
        assertTrue(post(newLeader, "/client/set", "key", "after", "value", "failover").success());
    }

    // ---------------------------------------------------------------- helpers

    private void seed(int nodeId, long term, LogEntry... entries) throws IOException {
        Path dir = tempDir.resolve("node" + nodeId);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("raft.state"), "term=" + term + "\nvotedFor=\n");
        try (PersistentLog log = new PersistentLog(dir.toString())) {
            log.init();
            for (LogEntry e : entries) log.append(e);
        }
    }

    private void startCluster(int size) throws Exception {
        startCluster(size, NodeConfig.DEFAULT_SNAPSHOT_THRESHOLD);
    }

    private void startCluster(int size, int snapshotThreshold) throws Exception {
        for (int id = 1; id <= size; id++) {
            if (!portById.containsKey(id)) portById.put(id, freePort());
        }
        for (int id = 1; id <= size; id++) {
            Map<Integer, NodeConfig.PeerAddress> peers = new HashMap<>();
            for (int other = 1; other <= size; other++) {
                if (other != id) peers.put(other, new NodeConfig.PeerAddress(other, "localhost", portById.get(other)));
            }
            NodeConfig config = new NodeConfig(id, portById.get(id), tempDir.resolve("node" + id), peers, snapshotThreshold);
            configById.put(id, config);
            RaftNode node = new RaftNode(config);
            node.start();
            nodes.add(node);
        }
    }

    private long termOf(RaftNode node) throws Exception {
        String status = http.send(HttpRequest.newBuilder(uri(node, "/status")).GET().build(),
            HttpResponse.BodyHandlers.ofString()).body();
        for (String part : status.split(" ")) {
            if (part.startsWith("term=")) return Long.parseLong(part.substring(5));
        }
        throw new IllegalStateException("no term in status: " + status);
    }

    /** Waits for a leader and for every node to have seen its no-op, i.e. the cluster has settled. */
    private RaftNode awaitLeader() throws InterruptedException {
        awaitAllNodesCommitted(1);
        return awaitLeaderOtherThan(null);
    }

    private RaftNode awaitLeaderOtherThan(RaftNode excluded) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : nodes) {
                if (n != excluded && n.role() == NodeRole.LEADER) return n;
            }
            Thread.sleep(50);
        }
        return fail("no leader elected within 10s");
    }

    private void awaitAllNodesKnowLeader() throws InterruptedException {
        awaitCondition("all nodes to learn the leader", () -> nodes.stream().allMatch(n -> n.currentLeaderId() != null));
    }

    private void awaitAllNodesCommitted(long index) throws InterruptedException {
        awaitCondition("all nodes to reach commitIndex " + index, () -> nodes.stream().allMatch(n -> n.commitIndex() >= index));
    }

    private void awaitCondition(String what, Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) return;
            Thread.sleep(50);
        }
        StringBuilder status = new StringBuilder();
        for (RaftNode n : nodes) status.append("\n  ").append(n.statusSummary());
        fail("timed out waiting for " + what + status);
    }

    /** Reads from a leader, retrying while it is still settling (e.g. committing its no-op). */
    private String awaitGet(RaftNode leader, String key) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        ClientResponse last = null;
        while (System.currentTimeMillis() < deadline) {
            last = get(leader, key);
            if (last.success()) return last.value();
            Thread.sleep(50);
        }
        return fail("read never succeeded: " + last);
    }

    private ClientResponse post(RaftNode node, String path, Object... kv) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) body.put((String) kv[i], kv[i + 1]);
        HttpRequest req = HttpRequest.newBuilder(uri(node, path))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(Json.encode(body)))
            .build();
        return ClientResponse.fromJson(http.send(req, HttpResponse.BodyHandlers.ofString()).body());
    }

    private ClientResponse get(RaftNode node, String key) throws Exception {
        String q = URLEncoder.encode(key, StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(uri(node, "/client/get?key=" + q))
            .timeout(Duration.ofSeconds(5)).GET().build();
        return ClientResponse.fromJson(http.send(req, HttpResponse.BodyHandlers.ofString()).body());
    }

    private URI uri(RaftNode node, String path) {
        return URI.create("http://localhost:" + portById.get(node.nodeId()) + path);
    }

    /** A free port below the ephemeral range, so outgoing connections can never be handed the same number. */
    private static int freePort() {
        for (int attempt = 0; attempt < 100; attempt++) {
            int port = 20_000 + ThreadLocalRandom.current().nextInt(20_000);
            try (ServerSocket s = new ServerSocket(port)) {
                return port;
            } catch (IOException busy) {
                // try another
            }
        }
        throw new IllegalStateException("no free port found");
    }
}
