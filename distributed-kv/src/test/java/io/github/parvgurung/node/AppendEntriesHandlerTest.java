package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.rpc.AppendEntriesRequest;
import io.github.parvgurung.rpc.AppendEntriesResponse;
import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Drives a follower's AppendEntries handler directly. Its peers are unreachable, and the handler keeps resetting the election timer. */
class AppendEntriesHandlerTest {
    @TempDir
    Path tempDir;

    private RaftNode node;

    @BeforeEach
    void start() throws IOException {
        Map<Integer, NodeConfig.PeerAddress> peers = Map.of(
            2, new NodeConfig.PeerAddress(2, "localhost", 1),
            3, new NodeConfig.PeerAddress(3, "localhost", 1));
        node = new RaftNode(new NodeConfig(1, freePort(), tempDir, peers));
        node.start();
    }

    @AfterEach
    void stop() throws IOException {
        node.shutdown();
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.set("k" + index, "v" + index));
    }

    @Test
    void commitIndexIsCappedAtTheLastEntryTheRequestVerified() {
        // Follower holds entries 1-3, but entries 2-3 are an unverified suffix from the follower's point of view.
        assertTrue(node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0,
            List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0)).success());

        // Heartbeat that only vouches for index 1, with a leaderCommit that is further ahead.
        AppendEntriesResponse r = node.handleAppendEntries(new AppendEntriesRequest(1, 2, 1, 1, List.of(), 3));

        assertTrue(r.success());
        assertEquals(1, node.commitIndex());
    }

    @Test
    void commitIndexAdvancesToLeaderCommitWhenRequestCoversIt() {
        node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0, List.of(entry(1, 1), entry(2, 1)), 2));
        assertEquals(2, node.commitIndex());
    }

    @Test
    void commitIndexNeverMovesBackward() {
        node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0, List.of(entry(1, 1), entry(2, 1)), 2));
        node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0, List.of(), 0));
        assertEquals(2, node.commitIndex());
    }

    @Test
    void mismatchedPrevEntryReportsAUsableConflictIndex() {
        node.handleAppendEntries(new AppendEntriesRequest(1, 2, 0, 0, List.of(entry(1, 1)), 0));

        AppendEntriesResponse r = node.handleAppendEntries(new AppendEntriesRequest(1, 2, 5, 1, List.of(), 0));

        assertFalse(r.success());
        assertTrue(r.conflictIndex() > 0, "a real log mismatch must report conflictIndex > 0");
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
