package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PersistentLogCompactionTest {
    @TempDir
    Path tempDir;

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.set("k" + index, "v" + index));
    }

    private PersistentLog logWith(int count, long term) throws IOException {
        PersistentLog log = new PersistentLog(tempDir.toString());
        log.init();
        for (int i = 1; i <= count; i++) log.append(entry(i, term));
        return log;
    }

    @Test
    void compactingKeepsTheTailAndTheIndexing() throws IOException {
        try (PersistentLog log = logWith(10, 2)) {
            log.compactThrough(6, 2);

            assertEquals(6, log.snapshotIndex());
            assertEquals(2, log.snapshotTerm());
            assertEquals(10, log.lastIndex());
            assertTrue(log.get(6).isEmpty(), "entry 6 is inside the snapshot");
            assertEquals("k7", log.get(7).orElseThrow().command().key());
            assertEquals(10, log.get(10).orElseThrow().index());
            assertTrue(log.get(11).isEmpty());
        }
    }

    @Test
    void termAtCoversTheSnapshotBoundary() throws IOException {
        try (PersistentLog log = logWith(5, 3)) {
            assertEquals(0L, log.termAt(0).orElseThrow(), "index 0 has term 0");
            log.compactThrough(4, 3);
            assertEquals(3L, log.termAt(4).orElseThrow());
            assertEquals(3L, log.termAt(5).orElseThrow());
            assertTrue(log.termAt(3).isEmpty(), "compacted away");
            assertTrue(log.termAt(6).isEmpty(), "does not exist yet");
        }
    }

    @Test
    void fullyCompactedLogStillKnowsItsLastIndexAndTerm() throws IOException {
        try (PersistentLog log = logWith(5, 3)) {
            log.compactThrough(5, 3);
            assertEquals(5, log.lastIndex());
            assertEquals(3, log.lastTerm());
            log.append(entry(6, 4));
            assertEquals(6, log.lastIndex());
            assertEquals(4, log.lastTerm());
        }
    }

    @Test
    void appendsAfterCompactionContinueFromTheBase() throws IOException {
        try (PersistentLog log = logWith(5, 1)) {
            log.compactThrough(5, 1);
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(5, 1)));
            log.append(entry(6, 1));
            assertEquals(6, log.lastIndex());
        }
    }

    @Test
    void compactionSurvivesRestartWhenInitialisedWithTheSnapshotBase() throws IOException {
        try (PersistentLog log = logWith(8, 2)) {
            log.compactThrough(5, 2);
        }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init(5, 2);
            assertEquals(8, log.lastIndex());
            assertEquals("k6", log.get(6).orElseThrow().command().key());
            assertTrue(log.get(5).isEmpty());
        }
    }

    @Test
    void entriesTheSnapshotAlreadyCoversAreDroppedOnRecovery() throws IOException {
        // Crash after the snapshot was saved but before the log was compacted.
        try (PersistentLog log = logWith(8, 2)) { }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init(5, 2);
            assertEquals(5, log.snapshotIndex());
            assertEquals(8, log.lastIndex());
            assertTrue(log.get(5).isEmpty());
        }
        String onDisk = Files.readString(tempDir.resolve("raft.log"));
        assertFalse(onDisk.contains("k1|") || onDisk.contains("k5|"), "stale entries were left in the file:\n" + onDisk);
    }

    @Test
    void gapBetweenSnapshotAndLogIsRejected() throws IOException {
        // The log starts at index 7, but the snapshot only reaches 5: entries 6 is missing.
        Files.writeString(tempDir.resolve("raft.log"),
            entry(7, 1).serialize() + "\n" + entry(8, 1).serialize() + "\n", StandardCharsets.UTF_8);
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            assertThrows(IllegalStateException.class, () -> log.init(5, 1));
        }
    }

    @Test
    void snapshotNewerThanTheWholeLogDiscardsIt() throws IOException {
        try (PersistentLog log = logWith(3, 1)) {
            log.compactThrough(10, 4); // e.g. installed from the leader; we never had entries 4..10
            assertEquals(10, log.snapshotIndex());
            assertEquals(10, log.lastIndex());
            assertEquals(4, log.lastTerm());
            assertTrue(log.get(3).isEmpty());
            log.append(entry(11, 4));
        }
    }

    @Test
    void snapshotThatDisagreesWithTheLogDiscardsIt() throws IOException {
        try (PersistentLog log = logWith(6, 1)) {
            log.compactThrough(4, 2); // our entry 4 has term 1, the snapshot says term 2
            assertEquals(4, log.lastIndex(), "the divergent entries 5-6 must not survive");
            assertEquals(2, log.lastTerm());
        }
    }

    @Test
    void compactingBackwardsIsIgnored() throws IOException {
        try (PersistentLog log = logWith(6, 1)) {
            log.compactThrough(4, 1);
            log.compactThrough(2, 1);
            assertEquals(4, log.snapshotIndex());
            assertEquals(6, log.lastIndex());
        }
    }

    @Test
    void truncateStillWorksAboveTheBaseButNotInsideTheSnapshot() throws IOException {
        try (PersistentLog log = logWith(8, 1)) {
            log.compactThrough(4, 1);
            assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(4));
            log.truncateFrom(6);
            assertEquals(5, log.lastIndex());
        }
    }

    @Test
    void candidateUpToDateCheckUsesTheBaseWhenTheLogIsEmpty() throws IOException {
        try (PersistentLog log = logWith(5, 2)) {
            log.compactThrough(5, 2);
            assertTrue(log.isCandidateLogUpToDate(5, 2));
            assertFalse(log.isCandidateLogUpToDate(4, 2));
            assertFalse(log.isCandidateLogUpToDate(9, 1));
            assertTrue(log.isCandidateLogUpToDate(1, 3));
        }
    }
}
