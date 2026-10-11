package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.rpc.AppendEntriesRequest;
import io.github.parvgurung.rpc.RequestVoteRequest;
import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Election safety depends on a node never voting twice in a term, even if it crashes between the two requests. */
class VotePersistenceTest {
    @TempDir
    Path tempDir;

    private RaftNode node;

    @AfterEach
    void stop() throws IOException {
        if (node != null) node.shutdown();
    }

    private void boot() throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        node = new RaftNode(new NodeConfig(1, port, tempDir, Map.of(
            2, new NodeConfig.PeerAddress(2, "localhost", 1),
            3, new NodeConfig.PeerAddress(3, "localhost", 1)), 0));
        node.start();
    }

    private void restart() throws IOException {
        node.shutdown();
        boot();
    }

    @Test
    void aVoteGivenBeforeACrashStillCountsAfterIt() throws IOException {
        boot();
        assertTrue(node.handleRequestVote(new RequestVoteRequest(5, 2, 0, 0)).voteGranted());

        restart();

        assertEquals(5, node.currentTerm(), "the term must survive a restart");
        assertFalse(node.handleRequestVote(new RequestVoteRequest(5, 3, 0, 0)).voteGranted(),
            "voted for node 2 in term 5 already; must not vote for node 3 in the same term after a restart");
        assertTrue(node.handleRequestVote(new RequestVoteRequest(5, 2, 0, 0)).voteGranted(),
            "asking again for the same candidate is fine (the first reply may have been lost)");
        assertTrue(node.handleRequestVote(new RequestVoteRequest(6, 3, 0, 0)).voteGranted(), "a newer term frees the vote");
    }

    @Test
    void aTermAdoptedFromALeaderSurvivesARestart() throws IOException {
        boot();
        node.handleAppendEntries(new AppendEntriesRequest(7, 2, 0, 0, List.of(), 0));
        restart();
        assertEquals(7, node.currentTerm());
        // and an old leader from term 6 is still refused afterwards
        assertFalse(node.handleAppendEntries(new AppendEntriesRequest(6, 3, 0, 0, List.of(), 0)).success());
    }

    @Test
    void entriesAcknowledgedBeforeACrashAreStillThereAfterIt() throws IOException {
        boot();
        List<LogEntry> entries = List.of(
            new LogEntry(1, 1, Command.set("a", "1")),
            new LogEntry(2, 1, Command.set("b", "2")));
        assertTrue(node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0, entries, 0)).success());

        restart();

        assertEquals(2, node.lastLogIndex());
        // a candidate with a shorter log must not win our vote: we remember what we acknowledged
        assertFalse(node.handleRequestVote(new RequestVoteRequest(2, 3, 1, 1)).voteGranted());
        assertTrue(node.handleRequestVote(new RequestVoteRequest(2, 3, 2, 1)).voteGranted());
    }
}
