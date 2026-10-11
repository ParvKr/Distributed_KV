package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.rpc.AppendEntriesRequest;
import io.github.parvgurung.rpc.RequestVoteRequest;
import io.github.parvgurung.rpc.RequestVoteResponse;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.util.Json;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The voting side of PreVote, driven directly. Peers are unreachable, and the follower's timer is far from firing. */
class PreVoteHandlerTest {
    @TempDir
    Path tempDir;

    private RaftNode node;

    @AfterEach
    void stop() throws IOException {
        if (node != null) node.shutdown();
    }

    private void startFollower() throws IOException {
        Map<Integer, NodeConfig.PeerAddress> peers = Map.of(
            2, new NodeConfig.PeerAddress(2, "localhost", 1),
            3, new NodeConfig.PeerAddress(3, "localhost", 1));
        node = new RaftNode(new NodeConfig(1, freePort(), tempDir, peers, 0));
        node.start();
    }

    private long termOf(RaftNode n) {
        return Json.getLong(Json.decode(n.statusJson()), "term");
    }

    private static RequestVoteRequest preVote(long term, long candidate, long lastIndex, long lastTerm) {
        return new RequestVoteRequest(term, candidate, lastIndex, lastTerm, true);
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.set("k" + index, "v"));
    }

    private void heartbeatFromLeader(long term) {
        node.handleAppendEntries(new AppendEntriesRequest(term, 2, 0, 0, List.of(), 0));
    }

    @Test
    void grantsWhenTheCandidateCouldWinAndNoLeaderIsAround() throws IOException {
        startFollower();
        RequestVoteResponse r = node.handleRequestVote(preVote(1, 2, 0, 0));
        assertTrue(r.voteGranted());
    }

    @Test
    void answeringChangesNothing() throws IOException {
        startFollower();
        node.handleRequestVote(preVote(5, 2, 0, 0));

        assertEquals(0, termOf(node), "a pre-vote must not move our term");
        // ...and it must not have recorded a vote either: a real vote for a different candidate is still free.
        RequestVoteResponse real = node.handleRequestVote(new RequestVoteRequest(5, 3, 0, 0));
        assertTrue(real.voteGranted());
        assertEquals(5, termOf(node));
    }

    @Test
    void deniesWhileALeaderIsStillBeingHeard() throws Exception {
        startFollower();
        heartbeatFromLeader(1);

        assertFalse(node.handleRequestVote(preVote(2, 3, 0, 0)).voteGranted(),
            "we just heard from a leader, so there is no reason for an election");

        Thread.sleep(450); // longer than the leader-stickiness window, shorter than our election timeout
        assertTrue(node.handleRequestVote(preVote(2, 3, 0, 0)).voteGranted());
    }

    @Test
    void deniesACandidateWhoseLogIsBehind() throws IOException {
        startFollower();
        node.handleAppendEntries(new AppendEntriesRequest(3, 2, 0, 0, List.of(entry(1, 3), entry(2, 3)), 0));
        // let the leader-stickiness window pass is not needed: log check fails regardless
        assertFalse(node.handleRequestVote(preVote(4, 3, 1, 2)).voteGranted());
    }

    @Test
    void deniesWhenTheProposedTermIsNotNewer() throws IOException {
        startFollower();
        node.handleRequestVote(new RequestVoteRequest(4, 3, 0, 0)); // moves us to term 4
        assertFalse(node.handleRequestVote(preVote(4, 2, 0, 0)).voteGranted());
        assertFalse(node.handleRequestVote(preVote(3, 2, 0, 0)).voteGranted());
        assertTrue(node.handleRequestVote(preVote(5, 2, 0, 0)).voteGranted());
    }

    @Test
    void aLeaderNeverGrantsAPreVote() throws Exception {
        node = new RaftNode(new NodeConfig(1, freePort(), tempDir, Map.of(), 0));
        node.start();
        long deadline = System.currentTimeMillis() + 5000;
        while (node.role() != NodeRole.LEADER && System.currentTimeMillis() < deadline) Thread.sleep(25);
        assertEquals(NodeRole.LEADER, node.role());

        assertFalse(node.handleRequestVote(preVote(99, 2, 99, 99)).voteGranted());
        assertEquals(NodeRole.LEADER, node.role(), "a pre-vote must not unseat a leader");
    }

    @Test
    void preVoteFlagRoundTripsAndDefaultsToARealVote() {
        RequestVoteRequest pre = preVote(3, 2, 7, 1);
        assertEquals(pre, RequestVoteRequest.fromJson(pre.toJson()));
        assertTrue(RequestVoteRequest.fromJson(pre.toJson()).preVote());

        // A message from a peer that predates PreVote has no flag.
        String old = "{\"term\":3,\"candidateId\":2,\"lastLogIndex\":7,\"lastLogTerm\":1}";
        assertFalse(RequestVoteRequest.fromJson(old).preVote());
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
