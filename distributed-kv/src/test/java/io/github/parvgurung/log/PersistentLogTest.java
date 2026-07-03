package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PersistentLogTest {

    @TempDir
    Path tempDir;

    @Test
    void testInitCreatesEmptyLog() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            assertEquals(0, log.lastIndex());
            assertEquals(0, log.lastTerm());
            assertTrue(log.get(1).isEmpty());
        }
    }

    @Test
    void testAppendSingleEntry() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            LogEntry entry = new LogEntry(1, 1, Command.set("a", "1"));

            log.append(entry);

            assertEquals(1, log.lastIndex());
            assertEquals(1, log.lastTerm());
            assertEquals(Optional.of(entry), log.get(1));
        }
    }

    @Test
    void testAppendMultipleEntries() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            for (int i = 1; i <= 10; i++) {
                log.append(new LogEntry(i, 1, Command.set("k" + i, "v" + i)));
            }

            assertEquals(10, log.lastIndex());
            assertEquals(1, log.lastTerm());

            assertTrue(log.get(5).isPresent());
            assertEquals("k5", log.get(5).get().command().key());
        }
    }

    @Test
    void testReloadFromDisk() throws IOException {

        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 1, Command.set("x", "10")));
            log.append(new LogEntry(2, 2, Command.set("y", "20")));
        }

        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            assertEquals(2, log.lastIndex());
            assertEquals(2, log.lastTerm());

            assertEquals(
                    "10",
                    log.get(1).get().command().value()
            );

            assertEquals(
                    "20",
                    log.get(2).get().command().value()
            );
        }
    }

    @Test
    void testOutOfOrderAppendThrows() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> log.append(
                            new LogEntry(2, 1, Command.set("a", "1"))
                    )
            );
        }
    }

    @Test
    void testDuplicateAppendThrows() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 1, Command.set("a", "1")));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> log.append(
                            new LogEntry(1, 1, Command.set("b", "2"))
                    )
            );
        }
    }

    @Test
    void testTruncateLastEntry() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 1, Command.set("a", "1")));
            log.append(new LogEntry(2, 1, Command.set("b", "2")));
            log.append(new LogEntry(3, 1, Command.set("c", "3")));

            log.truncateFrom(3);

            assertEquals(2, log.lastIndex());
            assertTrue(log.get(3).isEmpty());
        }
    }

    @Test
    void testAppendAfterTruncate() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 1, Command.set("a", "1")));
            log.append(new LogEntry(2, 1, Command.set("b", "2")));

            log.truncateFrom(2);

            log.append(new LogEntry(2, 2, Command.set("new", "value")));

            assertEquals(2, log.lastIndex());
            assertEquals(2, log.lastTerm());

            assertEquals(
                    "new",
                    log.get(2).get().command().key()
            );
        }
    }

    @Test
    void testGetMissingEntry() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            assertTrue(log.get(100).isEmpty());
        }
    }

    @Test
    void testCandidateWithHigherTermIsUpToDate() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 2, Command.set("a", "1")));

            assertTrue(
                    log.isCandidateLogUpToDate(1, 3)
            );
        }
    }

    @Test
    void testCandidateWithLowerTermIsNotUpToDate() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 5, Command.set("a", "1")));

            assertFalse(
                    log.isCandidateLogUpToDate(100, 4)
            );
        }
    }

    @Test
    void testCandidateSameTermHigherIndex() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 2, Command.set("a", "1")));

            assertTrue(
                    log.isCandidateLogUpToDate(2, 2)
            );
        }
    }

    @Test
    void testCandidateSameTermLowerIndex() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();

            log.append(new LogEntry(1, 2, Command.set("a", "1")));
            log.append(new LogEntry(2, 2, Command.set("b", "2")));

            assertFalse(
                    log.isCandidateLogUpToDate(1, 2)
            );
        }
    }

    @Test
    void testCorruptedLogDetection() throws IOException {

        Path logFile = tempDir.resolve("raft.log");

        Files.writeString(
                logFile,
                """
                1 | 1 | SET|a|1
                3 | 1 | SET|b|2
                """,
                StandardCharsets.UTF_8
        );

        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            assertThrows(
                    IllegalStateException.class,
                    log::init
            );
        }
    }
}