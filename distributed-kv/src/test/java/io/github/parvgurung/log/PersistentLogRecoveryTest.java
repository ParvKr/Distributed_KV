package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PersistentLogRecoveryTest {
    @TempDir
    Path tempDir;

    @Test
    void tornFinalWriteIsDroppedOnRecovery() throws IOException {
        Path file = tempDir.resolve("raft.log");
        String good = new LogEntry(1, 1, Command.set("a", "1")).serialize() + "\n";
        String torn = new LogEntry(2, 1, Command.set("b", "2")).serialize().substring(0, 10); // crash mid-write
        Files.writeString(file, good + torn, StandardCharsets.UTF_8);

        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            assertEquals(1, log.lastIndex());
            assertEquals(good, Files.readString(file), "torn bytes must be removed from the file");

            log.append(new LogEntry(2, 1, Command.set("b", "2")));
        }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            assertEquals(2, log.lastIndex());
        }
    }

    @Test
    void truncationSurvivesRestartAndLeavesNoTempFile() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            for (int i = 1; i <= 4; i++) log.append(new LogEntry(i, 1, Command.set("k" + i, "v")));
            log.truncateFrom(3);
            log.append(new LogEntry(3, 2, Command.set("new", "v")));
        }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            assertEquals(3, log.lastIndex());
            assertEquals(2, log.lastTerm());
        }
        assertFalse(Files.exists(tempDir.resolve("raft.log.tmp")));
    }

    @Test
    void truncatingFromIndexZeroIsRejected() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            log.append(new LogEntry(1, 1, Command.set("a", "1")));
            assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(0));
            assertEquals(1, log.lastIndex());
        }
    }
}
