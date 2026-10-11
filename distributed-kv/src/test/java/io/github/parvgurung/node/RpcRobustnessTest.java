package io.github.parvgurung.node;

import io.github.parvgurung.rpc.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hostile or buggy input to every endpoint. None of it may crash a handler (HTTP 5xx), corrupt a node, or stop
 * the cluster from serving normal traffic afterwards.
 */
class RpcRobustnessTest {
    @TempDir
    Path tempDir;

    private TestCluster cluster;
    private RaftNode leader;
    private RaftNode follower;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(3, tempDir, 0);
        cluster.startAll();
        leader = cluster.awaitLeader(10_000);
        cluster.awaitCondition("settled", 10_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));
        leader = cluster.awaitLeader(10_000);
        follower = cluster.runningNodes().stream().filter(n -> n != leader).findFirst().orElseThrow();
    }

    @AfterEach
    void stop() throws Exception {
        cluster.close();
    }

    // Failures are collected so one run shows every problem, not just the first.
    private final List<String> problems = new ArrayList<>();

    private void assertHandled(String label, HttpResponse<String> r) {
        if (r.statusCode() >= 500) problems.add(label + " -> HTTP " + r.statusCode() + " " + r.body());
    }

    private void assertRejected(String label, HttpResponse<String> r) {
        if (!(r.statusCode() == 400 || r.statusCode() == 415 || r.statusCode() == 405))
            problems.add(label + " should be rejected as a bad request but was HTTP " + r.statusCode() + " " + r.body());
    }

    private void assertNoProblems() {
        assertTrue(problems.isEmpty(), problems.size() + " problem(s):\n" + String.join("\n", problems));
    }

    private void assertClusterStillWorks() throws Exception {
        RaftNode current = cluster.awaitLeader(10_000);
        ClientResponse r = cluster.set(current, "still", "works", null, 0);
        assertTrue(r.success(), "cluster stopped serving writes after bad input: " + r.error() + "\n" + cluster.describe());
        for (RaftNode n : cluster.runningNodes()) {
            HttpResponse<String> status = cluster.raw(n.nodeId(), "GET", "/status.json", null, null);
            assertEquals(200, status.statusCode());
        }
    }

    // ------------------------------------------------------------------ syntactically bad bodies

    private static final List<String> NOT_OBJECTS = List.of(
        "", "   ", "not json", "[]", "[1,2,3]", "null", "true", "42", "\"string\"", "{", "}", "{\"a\":", "{\"a\":1,}",
        "{\"a\" 1}", "{'single':'quotes'}", "\u0000\u0001\u0002",
        "{\"a\":" + "[".repeat(50_000), "{\"a\":" + "{\"b\":".repeat(50_000) + "1");

    @Test
    void everyEndpointRejectsBodiesThatAreNotJsonObjects() throws Exception {
        for (RaftNode target : List.of(leader, follower)) {
            for (String path : List.of("/raft/requestVote", "/raft/appendEntries", "/raft/installSnapshot", "/client/set", "/client/delete")) {
                for (String body : NOT_OBJECTS) {
                    HttpResponse<String> r = cluster.raw(target.nodeId(), "POST", path, "application/json", body);
                    assertRejected(path + " body=" + (body.length() > 30 ? body.substring(0, 30) + "..." : body), r);
                }
            }
        }
        assertNoProblems();
        assertClusterStillWorks();
    }

    // ------------------------------------------------------------------ well-formed JSON, wrong content

    @Test
    void requestVoteWithBadFieldsIsRejectedOrHarmless() throws Exception {
        List<String> bodies = List.of(
            "{}",
            "{\"term\":\"1\",\"candidateId\":1,\"lastLogIndex\":0,\"lastLogTerm\":0}",
            "{\"term\":1,\"candidateId\":null,\"lastLogIndex\":0,\"lastLogTerm\":0}",
            "{\"term\":1.5,\"candidateId\":1,\"lastLogIndex\":0,\"lastLogTerm\":0}",
            "{\"term\":-5,\"candidateId\":1,\"lastLogIndex\":0,\"lastLogTerm\":0}",
            "{\"term\":0,\"candidateId\":1099511627776,\"lastLogIndex\":0,\"lastLogTerm\":0}",        // does not fit an int
            "{\"term\":0,\"candidateId\":-9223372036854775808,\"lastLogIndex\":-1,\"lastLogTerm\":-1}",
            "{\"term\":0,\"candidateId\":1,\"lastLogIndex\":9223372036854775807,\"lastLogTerm\":9223372036854775807,\"preVote\":\"yes\"}");
        for (RaftNode target : List.of(leader, follower)) {
            for (String body : bodies) {
                assertHandled("requestVote " + body, cluster.raw(target.nodeId(), "POST", "/raft/requestVote", "application/json", body));
            }
        }
        assertNoProblems();
        assertClusterStillWorks();
    }

    @Test
    void appendEntriesWithBadFieldsIsRejectedOrHarmless() throws Exception {
        long term = follower.currentTerm();
        long lid = leader.nodeId();
        String head = "{\"term\":" + term + ",\"leaderId\":" + lid;
        List<String> bodies = List.of(
            "{}",
            head + "}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":5}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[1,\"x\",null]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"EXPLODE\",\"key\":\"k\",\"value\":\"v\"}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{\"index\":1,\"term\":0,\"commandType\":\"SET\",\"key\":\"k\",\"value\":\"v\"}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"SET\",\"key\":\"\",\"value\":\"v\"}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"SET\",\"key\":\"k\"}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"SET\",\"key\":\"\\ud83d\",\"value\":\"v\"}]}",
            head + ",\"prevLogIndex\":-1,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[]}",
            head + ",\"prevLogIndex\":-9223372036854775808,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[]}",
            head + ",\"prevLogIndex\":-9223372036854775808,\"prevLogTerm\":0,\"leaderCommit\":9223372036854775807,"
                + "\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"SET\",\"key\":\"k\",\"value\":\"v\"}]}",
            head + ",\"prevLogIndex\":9223372036854775807,\"prevLogTerm\":1,\"leaderCommit\":9223372036854775807,\"entries\":[]}",
            head + ",\"prevLogIndex\":9223372036854775807,\"prevLogTerm\":1,\"leaderCommit\":0,"
                + "\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"NOOP\",\"key\":\"\",\"value\":null}]}",
            head + ",\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":-7,\"entries\":[]}");
        for (String body : bodies) {
            assertHandled("appendEntries " + body, cluster.raw(follower.nodeId(), "POST", "/raft/appendEntries", "application/json", body));
        }
        assertNoProblems();
        assertClusterStillWorks();
        // and the follower's log must not have been polluted by any of that
        cluster.awaitCondition("convergence", 10_000, cluster::converged);
        assertFalse(cluster.get(cluster.awaitLeader(5000), "k").success() && cluster.get(cluster.awaitLeader(5000), "k").value() != null,
            "a rejected entry leaked into the state machine");
    }

    @Test
    void installSnapshotWithBadFieldsIsRejectedOrHarmless() throws Exception {
        long term = follower.currentTerm();
        String head = "{\"term\":" + term + ",\"leaderId\":" + leader.nodeId() + ",\"snapshot\":";
        List<String> bodies = List.of(
            head + "null}",
            head + "5}",
            head + "{}}",
            head + "{\"lastIncludedIndex\":0,\"lastIncludedTerm\":1,\"data\":{},\"sessions\":{}}}",
            head + "{\"lastIncludedIndex\":5,\"lastIncludedTerm\":0,\"data\":{},\"sessions\":{}}}",
            head + "{\"lastIncludedIndex\":-5,\"lastIncludedTerm\":1,\"data\":{},\"sessions\":{}}}",
            head + "{\"lastIncludedIndex\":5,\"lastIncludedTerm\":1,\"data\":{\"k\":7},\"sessions\":{}}}",
            head + "{\"lastIncludedIndex\":5,\"lastIncludedTerm\":1,\"data\":[],\"sessions\":{}}}",
            head + "{\"lastIncludedIndex\":5,\"lastIncludedTerm\":1,\"data\":{},\"sessions\":{\"c\":\"x\"}}}",
            head + "{\"lastIncludedIndex\":5,\"lastIncludedTerm\":1,\"data\":{},\"sessions\":{\"c\":0}}}");
        for (String body : bodies) {
            assertHandled("installSnapshot " + body, cluster.raw(follower.nodeId(), "POST", "/raft/installSnapshot", "application/json", body));
        }
        assertNoProblems();
        assertClusterStillWorks();
    }

    @Test
    void clientWritesWithBadFieldsAreRejectedAndNeverApplied() throws Exception {
        List<String> bodies = List.of(
            "{}",
            "{\"key\":\"k\"}",                                                   // set without a value
            "{\"value\":\"v\"}",                                                 // no key
            "{\"key\":5,\"value\":\"v\"}",
            "{\"key\":\"k\",\"value\":5}",
            "{\"key\":\"k\",\"value\":null}",
            "{\"key\":\"\",\"value\":\"v\"}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"c\"}",                // session without seq
            "{\"key\":\"k\",\"value\":\"v\",\"seq\":3}",                         // seq without session
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"c\",\"seq\":0}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"c\",\"seq\":-4}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"c\",\"seq\":\"1\"}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"\",\"seq\":1}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":7,\"seq\":1}",
            "{\"key\":\"\\ud83d\",\"value\":\"v\"}",                             // unpaired surrogate
            "{\"key\":\"k\",\"value\":\"\\ude00\"}",
            "{\"key\":\"k\",\"value\":\"v\",\"clientId\":\"\\ud83d\",\"seq\":1}");
        for (String body : bodies) {
            assertRejected("set " + body, cluster.raw(leader.nodeId(), "POST", "/client/set", "application/json", body));
        }
        for (String body : List.of("{}", "{\"key\":5}", "{\"key\":\"\"}", "{\"key\":\"k\",\"clientId\":\"c\"}", "{\"key\":\"\\ud83d\"}")) {
            assertRejected("delete " + body, cluster.raw(leader.nodeId(), "POST", "/client/delete", "application/json", body));
        }
        assertNoProblems();
        cluster.awaitCondition("convergence", 10_000, cluster::converged);
        for (RaftNode n : cluster.runningNodes()) {
            assertTrue(n.stateSnapshot().data().isEmpty(), "node " + n.nodeId() + " applied a rejected write: " + n.stateSnapshot().data());
        }
        assertClusterStillWorks();
    }

    @Test
    void clientReadsWithBadQueriesAreRejected() throws Exception {
        for (String path : List.of("/client/get", "/client/get?", "/client/get?key", "/client/get?key=", "/client/get?other=1",
                "/client/get?key=%ZZ", "/client/get?key=%", "/client/get?key=%E0%A4%A", "/client/get?key=%FF%FE")) {
            int status = cluster.rawStatus(leader.nodeId(), "GET " + path);
            if (status != 400) problems.add("GET " + path + " should be 400 but was " + status);
        }
        assertNoProblems();
        assertClusterStillWorks();
    }

    // ------------------------------------------------------------------ wrong methods, unknown paths

    @Test
    void wrongMethodsAndUnknownPathsNeverReachTheStateMachine() throws Exception {
        List<String> methods = List.of("GET", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE");
        List<String> paths = new ArrayList<>(List.of("/raft/requestVote", "/raft/appendEntries", "/raft/installSnapshot",
            "/client/set", "/client/delete", "/client/get?key=k", "/nope", "/raft", "/client", "/../etc/passwd", "/status/../client/set"));
        for (String method : methods) {
            for (String path : paths) {
                HttpResponse<String> r;
                try {
                    r = cluster.raw(leader.nodeId(), method, path, "application/json", method.equals("GET") || method.equals("HEAD") ? null : "{\"key\":\"k\",\"value\":\"v\"}");
                } catch (IllegalArgumentException invalidUri) {
                    continue; // the client library itself refuses to send this one
                }
                if (r.statusCode() >= 500) problems.add(method + " " + path + " -> " + r.statusCode());
            }
        }
        assertNoProblems();
        cluster.awaitCondition("convergence", 10_000, cluster::converged);
        for (RaftNode n : cluster.runningNodes()) {
            assertTrue(n.stateSnapshot().data().isEmpty(), "a stray request wrote data on node " + n.nodeId());
        }
        assertClusterStillWorks();
    }

    // ------------------------------------------------------------------ size

    @Test
    void largeValuesAreReplicatedIntact() throws Exception {
        String big = "x".repeat(2 * 1024 * 1024);
        ClientResponse r = cluster.set(leader, "big", big, null, 0);
        assertTrue(r.success(), r.error());
        cluster.awaitCondition("replication", 20_000, cluster::converged);
        for (RaftNode n : cluster.runningNodes()) {
            assertEquals(big, n.stateSnapshot().data().get("big"), "node " + n.nodeId());
        }
    }

    // ------------------------------------------------------------------ cross-site forgery and size limits

    @Test
    void raftEndpointsRefuseRequestsThatABrowserCouldSendCrossSite() throws Exception {
        long before = follower.currentTerm();
        // What a malicious web page can send without any CORS preflight: a text/plain POST with a JSON-looking body.
        String forgedAppend = "{\"term\":" + (before + 1000) + ",\"leaderId\":3,\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,\"entries\":[]}";
        String forgedVote = "{\"term\":" + (before + 1000) + ",\"candidateId\":3,\"lastLogIndex\":99,\"lastLogTerm\":99}";
        for (String contentType : new String[]{"text/plain", "application/x-www-form-urlencoded", "multipart/form-data", null}) {
            assertEquals(415, cluster.raw(follower.nodeId(), "POST", "/raft/appendEntries", contentType, forgedAppend).statusCode(), String.valueOf(contentType));
            assertEquals(415, cluster.raw(follower.nodeId(), "POST", "/raft/requestVote", contentType, forgedVote).statusCode(), String.valueOf(contentType));
            assertEquals(415, cluster.raw(follower.nodeId(), "POST", "/raft/installSnapshot", contentType, "{}").statusCode(), String.valueOf(contentType));
        }
        for (String method : List.of("GET", "PUT", "DELETE")) {
            assertEquals(405, cluster.raw(follower.nodeId(), "GET".equals(method) ? "GET" : method, "/raft/appendEntries", "application/json", forgedAppend).statusCode(), method);
        }
        assertEquals(before, follower.currentTerm(), "a forged request changed the node's term");
        assertClusterStillWorks();
    }

    @Test
    void oversizedRequestBodiesAreRefusedWithoutBeingProcessed() throws Exception {
        String tooBig = "x".repeat(17 * 1024 * 1024);
        HttpResponse<String> r = cluster.raw(leader.nodeId(), "POST", "/client/set", "application/json",
            "{\"key\":\"k\",\"value\":\"" + tooBig + "\"}");
        assertEquals(413, r.statusCode(), r.body());
        cluster.awaitCondition("convergence", 10_000, cluster::converged);
        for (RaftNode n : cluster.runningNodes()) assertTrue(n.stateSnapshot().data().isEmpty());
        assertClusterStillWorks();
    }

    // ------------------------------------------------------------------ query decoding

    @Test
    void queryKeysAreDecodedAsStrictUtf8() throws Exception {
        assertTrue(cluster.set(leader, "\u65e5\u672c x", "found", null, 0).success());
        cluster.awaitCondition("settle", 5000, cluster::converged);
        for (String encoded : List.of("%E6%97%A5%E6%9C%AC+x", "%E6%97%A5%E6%9C%AC%20x", "\u65e5\u672c%20x")) {
            HttpResponse<String> r = cluster.raw(leader.nodeId(), "GET", "/client/get?key=" + encoded.replace("\u65e5\u672c", "%E6%97%A5%E6%9C%AC"), null, null);
            ClientResponse parsed = ClientResponse.fromJson(r.body());
            assertEquals("found", parsed.value(), encoded);
        }
    }

    // ------------------------------------------------------------------ a far-behind follower

    @Test
    void aFollowerThatIsThousandsOfEntriesBehindCatchesUpInBoundedChunks() throws Exception {
        TestCluster big = new TestCluster(3, tempDir.resolve("backlog"), 0); // snapshots off: the log itself must carry the backlog
        try {
            big.startAll();
            RaftNode current = big.awaitLeader(10_000);
            RaftNode sleeper = big.runningNodes().stream().filter(n -> n != current).findFirst().orElseThrow();
            int sleeperId = sleeper.nodeId();
            big.crash(sleeperId);

            String padding = "p".repeat(200);
            for (int i = 0; i < 1500; i++) {
                ClientResponse r = big.set(current, "key" + i, padding + i, null, 0);
                assertTrue(r.success(), "write " + i + ": " + r.error());
            }
            long termBefore = current.currentTerm();

            big.start(sleeperId);
            big.awaitCondition("the sleeper to catch up", 30_000, () -> big.node(sleeperId).commitIndex() >= current.commitIndex());
            big.awaitCondition("convergence", 10_000, big::converged);
            assertEquals(termBefore, current.currentTerm(), "catching up a lagging node must not disturb the cluster");
            assertEquals(1500, big.node(sleeperId).stateSnapshot().data().size());
        } finally {
            big.close();
        }
    }
}
