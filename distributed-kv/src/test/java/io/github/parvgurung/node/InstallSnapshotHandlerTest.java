package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.rpc.AppendEntriesRequest;
import io.github.parvgurung.rpc.AppendEntriesResponse;
import io.github.parvgurung.rpc.InstallSnapshotRequest;
import io.github.parvgurung.rpc.InstallSnapshotResponse;
import io.github.parvgurung.snapshot.Snapshot;
import io.github.parvgurung.snapshot.SnapshotStore;
import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Drives a follower's snapshot-related handlers directly. Its peers are unreachable. */
class InstallSnapshotHandlerTest {
    @TempDir
    Path tempDir;

    private RaftNode node;

    @BeforeEach
    void start() throws IOException {
        node = new RaftNode(newConfig());
        node.start();
    }

    @AfterEach
    void stop() throws IOException {
        node.shutdown();
    }

    private NodeConfig newConfig() throws IOException {
        Map<Integer, NodeConfig.PeerAddress> peers = Map.of(
            2, new NodeConfig.PeerAddress(2, "localhost", 1),
            3, new NodeConfig.PeerAddress(3, "localhost", 1));
        return new NodeConfig(1, freePort(), tempDir, peers, 0);
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.set("k" + index, "v" + index));
    }

    private static List<LogEntry> entries(long from, long to, long term) {
        List<LogEntry> list = new ArrayList<>();
        for (long i = from; i <= to; i++) list.add(entry(i, term));
        return list;
    }

    private static Snapshot snapshot(long index, long term) {
        return new Snapshot(index, term, Map.of("from-snapshot", "yes"), Map.of("client-A", 3L));
    }

    private static AppendEntriesRequest append(long term, long prevIndex, long prevTerm, List<LogEntry> entries, long commit) {
        return new AppendEntriesRequest(term, 2, prevIndex, prevTerm, entries, commit);
    }

    @Test
    void installingASnapshotAdvancesCommitAndSetsTheLogBase() {
        InstallSnapshotResponse r = node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        assertEquals(1, r.term());
        assertEquals(5, node.commitIndex());
        assertEquals(5, node.snapshotIndex());
        assertEquals(5, node.lastLogIndex());
        assertTrue(node.statusSummary().contains("entries=1"), node.statusSummary());
    }

    @Test
    void replicationContinuesRightAfterTheSnapshotBoundary() {
        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        AppendEntriesResponse r = node.handleAppendEntries(append(1, 5, 1, entries(6, 7, 1), 7));

        assertTrue(r.success());
        assertEquals(7, node.commitIndex());
        assertEquals(7, node.lastLogIndex());
    }

    @Test
    void appendEntriesOverlappingTheSnapshotSkipsTheCoveredPart() {
        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        // The leader still thinks we are at index 3, so it resends 4..8.
        AppendEntriesResponse r = node.handleAppendEntries(append(1, 3, 1, entries(4, 8, 1), 8));

        assertTrue(r.success(), "conflict=" + r.conflictIndex());
        assertEquals(8, node.commitIndex());
        assertEquals(8, node.lastLogIndex());
    }

    @Test
    void appendEntriesEntirelyInsideTheSnapshotIsHarmless() {
        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        AppendEntriesResponse r = node.handleAppendEntries(append(1, 1, 1, entries(2, 4, 1), 4));

        assertTrue(r.success());
        assertEquals(5, node.commitIndex());
        assertEquals(5, node.lastLogIndex());
    }

    @Test
    void mismatchAtTheSnapshotBoundaryIsReportedAsAConflict() {
        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        AppendEntriesResponse r = node.handleAppendEntries(append(1, 5, 9, entries(6, 6, 9), 0));

        assertFalse(r.success());
        assertTrue(r.conflictIndex() > 0);
    }

    @Test
    void staleSnapshotIsIgnored() {
        node.handleAppendEntries(append(1, 0, 0, entries(1, 8, 1), 8));
        assertEquals(8, node.commitIndex());

        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        assertEquals(8, node.commitIndex());
        assertEquals(0, node.snapshotIndex(), "an older snapshot must not replace what we already have");
        assertEquals(8, node.lastLogIndex());
    }

    @Test
    void snapshotFromAnOlderTermIsRejected() {
        node.handleAppendEntries(append(3, 0, 0, entries(1, 2, 3), 0)); // moves us to term 3

        InstallSnapshotResponse r = node.handleInstallSnapshot(new InstallSnapshotRequest(2, 2, snapshot(5, 2)));

        assertEquals(3, r.term());
        assertEquals(0, node.snapshotIndex());
        assertEquals(0, node.commitIndex());
    }

    @Test
    void matchingLogTailIsKeptWhenASnapshotIsInstalled() {
        node.handleAppendEntries(append(1, 0, 0, entries(1, 8, 1), 2));

        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));

        assertEquals(5, node.snapshotIndex());
        assertEquals(8, node.lastLogIndex(), "entries 6-8 agree with the snapshot and must survive");
        assertEquals(5, node.commitIndex());
    }

    @Test
    void divergentLogIsDiscardedWhenASnapshotIsInstalled() {
        node.handleAppendEntries(append(1, 0, 0, entries(1, 8, 1), 2));

        // The snapshot says entry 5 was written in term 2; ours came from term 1.
        node.handleInstallSnapshot(new InstallSnapshotRequest(2, 2, snapshot(5, 2)));

        assertEquals(5, node.lastLogIndex());
        assertEquals(5, node.commitIndex());
    }

    @Test
    void installedSnapshotIsPersistedAndSurvivesRestart() throws IOException {
        node.handleInstallSnapshot(new InstallSnapshotRequest(1, 2, snapshot(5, 1)));
        node.handleAppendEntries(append(1, 5, 1, entries(6, 7, 1), 7));
        node.shutdown();

        assertEquals(5, new SnapshotStore(tempDir.toString()).load().orElseThrow().lastIncludedIndex());

        node = new RaftNode(newConfig());
        node.start();
        assertEquals(5, node.snapshotIndex());
        assertEquals(7, node.lastLogIndex());
        assertEquals(5, node.commitIndex(), "commit index restarts at the snapshot and catches up from the leader");
        assertTrue(Files.exists(tempDir.resolve("snapshot")));
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
