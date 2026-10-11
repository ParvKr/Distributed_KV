package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PersistentLogFailureTest {
    @TempDir
    Path tempDir;

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.set("k" + index, "v" + index));
    }

    @Test
    void aFailedRewriteLeavesMemoryAndDiskTheSameAndTheLogUsable() throws IOException {
        assumeTrue(tempDir.getFileSystem().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            for (int i = 1; i <= 6; i++) log.append(entry(i, 1));

            // Make the directory read-only so the temp file for the atomic rewrite cannot be created.
            Files.setPosixFilePermissions(tempDir, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                assumeTrue(!Files.isWritable(tempDir), "running with privileges that ignore directory permissions");
                assertThrows(IOException.class, () -> log.compactThrough(4, 1));
                assertThrows(IOException.class, () -> log.truncateFrom(5));
            } finally {
                Files.setPosixFilePermissions(tempDir, PosixFilePermissions.fromString("rwxr-xr-x"));
            }

            // Nothing may have changed: the failed operations must not have eaten entries from memory.
            assertEquals(0, log.snapshotIndex());
            assertEquals(6, log.lastIndex());
            for (int i = 1; i <= 6; i++) assertEquals("k" + i, log.get(i).orElseThrow().command().key());

            // ...and the log still works.
            log.append(entry(7, 1));
            log.compactThrough(4, 1);
            assertEquals(7, log.lastIndex());
        }
        try (PersistentLog reopened = new PersistentLog(tempDir.toString())) {
            reopened.init(4, 1);
            assertEquals(7, reopened.lastIndex());
            assertEquals("k5", reopened.get(5).orElseThrow().command().key());
        }
    }

    @Test
    void batchAppendWritesEverythingAndSurvivesRestart() throws IOException {
        List<LogEntry> batch = new ArrayList<>();
        for (int i = 1; i <= 50; i++) batch.add(entry(i, 2));
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            log.appendAll(batch);
            log.appendAll(List.of());
            assertEquals(50, log.lastIndex());
        }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            assertEquals(50, log.lastIndex());
            assertEquals(batch.get(49), log.get(50).orElseThrow());
        }
    }

    @Test
    void aBatchWithAGapIsRejectedWholesale() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            log.append(entry(1, 1));
            assertThrows(IllegalArgumentException.class, () -> log.appendAll(List.of(entry(2, 1), entry(4, 1))));
            assertEquals(1, log.lastIndex(), "a rejected batch must not be partially applied");
            log.append(entry(2, 1)); // and the log is still appendable
        }
    }
}
