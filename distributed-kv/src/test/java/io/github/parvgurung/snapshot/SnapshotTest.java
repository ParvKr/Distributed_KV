package io.github.parvgurung.snapshot;

import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.statemachine.KVStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest {
    @TempDir
    Path tempDir;

    private static Snapshot sample() {
        return new Snapshot(42, 3,
            Map.of("plain", "value", "uni", "こんにちは \"quoted\" \n newline | pipe"),
            Map.of("client-A", 7L, "client-B", 1L));
    }

    @Test
    void jsonRoundTripPreservesEverything() {
        Snapshot original = sample();
        assertEquals(original, Snapshot.fromJson(original.toJson()));
    }

    @Test
    void emptyStateRoundTrips() {
        Snapshot original = new Snapshot(1, 1, Map.of(), Map.of());
        assertEquals(original, Snapshot.fromJson(original.toJson()));
    }

    @Test
    void rejectsNonPositiveIndexOrTerm() {
        assertThrows(IllegalArgumentException.class, () -> new Snapshot(0, 1, Map.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new Snapshot(1, 0, Map.of(), Map.of()));
    }

    @Test
    void rejectsNonStringValues() {
        String json = "{\"lastIncludedIndex\":1,\"lastIncludedTerm\":1,\"data\":{\"k\":5},\"sessions\":{}}";
        assertThrows(IllegalArgumentException.class, () -> Snapshot.fromJson(json));
    }

    @Test
    void storeSavesAndLoads() throws IOException {
        SnapshotStore store = new SnapshotStore(tempDir.toString());
        assertTrue(store.load().isEmpty());
        store.save(sample());
        assertEquals(sample(), store.load().orElseThrow());
    }

    @Test
    void storeOverwritesWithTheLatestSnapshotAndLeavesNoTempFile() throws IOException {
        SnapshotStore store = new SnapshotStore(tempDir.toString());
        store.save(sample());
        Snapshot newer = new Snapshot(100, 4, Map.of("only", "this"), Map.of());
        store.save(newer);
        assertEquals(newer, store.load().orElseThrow());
        assertFalse(Files.exists(tempDir.resolve("snapshot.tmp")));
    }

    @Test
    void corruptSnapshotFileIsReportedNotIgnored() throws IOException {
        Files.writeString(tempDir.resolve("snapshot"), "{not json", StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> new SnapshotStore(tempDir.toString()).load());
    }

    @Test
    void stateMachineCaptureAndRestoreRoundTrip() {
        KVStateMachine source = new KVStateMachine();
        source.apply(Command.set("a", "1", "A", 1), 1);
        source.apply(Command.set("b", "2"), 2);
        source.apply(Command.delete("a", "A", 2), 3);

        KVStateMachine copy = new KVStateMachine();
        copy.apply(Command.set("stale", "x"), 1); // gets replaced
        copy.restore(source.capture());

        assertEquals(3, copy.getLastAppliedIndex());
        assertEquals("2", copy.get("b").orElseThrow());
        assertTrue(copy.get("a").isEmpty());
        assertTrue(copy.get("stale").isEmpty());
        // dedup state came along: A's request 2 is already applied, request 3 is not
        assertTrue(copy.isDuplicate("A", 2));
        assertFalse(copy.isDuplicate("A", 3));
    }

    @Test
    void captureIsACopyNotAView() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("a", "1"), 1);
        KVStateMachine.State state = kv.capture();
        kv.apply(Command.set("a", "2"), 2);
        assertEquals("1", state.data().get("a"));
        assertEquals(1, state.lastAppliedIndex());
    }

    @Test
    void restoredStateMachineContinuesFromTheSnapshotIndex() {
        KVStateMachine kv = new KVStateMachine();
        kv.restore(new KVStateMachine.State(10, Map.of("k", "v"), Map.of()));
        assertThrows(IllegalStateException.class, () -> kv.apply(Command.set("x", "y"), 5));
        kv.apply(Command.set("x", "y"), 11);
        assertEquals(11, kv.getLastAppliedIndex());
    }
}
