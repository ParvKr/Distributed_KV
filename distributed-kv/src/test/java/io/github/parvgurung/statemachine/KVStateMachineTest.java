package io.github.parvgurung.statemachine;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class KVStateMachineTest {

    @Test
    void testInitialState() {
        KVStateMachine kv = new KVStateMachine();

        assertEquals(0, kv.size());
        assertEquals(0, kv.getLastAppliedIndex());
        assertEquals(Optional.empty(), kv.get("missing"));
    }

    @Test
    void testSetCommand() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("name", "Alice"), 1);

        assertEquals(Optional.of("Alice"), kv.get("name"));
        assertEquals(1, kv.size());
        assertEquals(1, kv.getLastAppliedIndex());
    }

    @Test
    void testDeleteCommand() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("name", "Alice"), 1);
        kv.apply(Command.delete("name"), 2);

        assertEquals(Optional.empty(), kv.get("name"));
        assertEquals(0, kv.size());
        assertEquals(2, kv.getLastAppliedIndex());
    }

    @Test
    void testOverwriteExistingValue() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("x", "1"), 1);
        kv.apply(Command.set("x", "2"), 2);

        assertEquals(Optional.of("2"), kv.get("x"));
        assertEquals(1, kv.size());
    }

    @Test
    void testDeleteMissingKey() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.delete("missing"), 1);

        assertEquals(Optional.empty(), kv.get("missing"));
        assertEquals(0, kv.size());
        assertEquals(1, kv.getLastAppliedIndex());
    }

    @Test
    void testMultipleKeys() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);
        kv.apply(Command.set("b", "2"), 2);
        kv.apply(Command.set("c", "3"), 3);

        assertEquals(Optional.of("1"), kv.get("a"));
        assertEquals(Optional.of("2"), kv.get("b"));
        assertEquals(Optional.of("3"), kv.get("c"));
        assertEquals(3, kv.size());
    }

    @Test
    void testSequentialOperations() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);
        kv.apply(Command.set("b", "2"), 2);
        kv.apply(Command.delete("a"), 3);
        kv.apply(Command.set("b", "20"), 4);

        assertEquals(Optional.empty(), kv.get("a"));
        assertEquals(Optional.of("20"), kv.get("b"));
        assertEquals(1, kv.size());
        assertEquals(4, kv.getLastAppliedIndex());
    }

    @Test
    void testGetMissingKey() {
        KVStateMachine kv = new KVStateMachine();

        assertTrue(kv.get("unknown").isEmpty());
    }

    @Test
    void testOutOfOrderIndexThrows() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);

        assertThrows(
                IllegalStateException.class,
                () -> kv.apply(Command.set("b", "2"), 3)
        );
    }

    @Test
    void testDuplicateIndexThrows() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);

        assertThrows(
                IllegalStateException.class,
                () -> kv.apply(Command.set("b", "2"), 1)
        );
    }

    @Test
    void testLowerIndexThrows() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);
        kv.apply(Command.set("b", "2"), 2);

        assertThrows(
                IllegalStateException.class,
                () -> kv.apply(Command.set("c", "3"), 1)
        );
    }

    @Test
    void testNullCommandThrows() {
        KVStateMachine kv = new KVStateMachine();

        assertThrows(
                NullPointerException.class,
                () -> kv.apply(null, 1)
        );
    }

    @Test
    void testLargeNumberOfCommands() {
        KVStateMachine kv = new KVStateMachine();

        for (int i = 1; i <= 1000; i++) {
            kv.apply(Command.set("k" + i, "v" + i), i);
        }

        assertEquals(1000, kv.size());
        assertEquals(1000, kv.getLastAppliedIndex());

        assertEquals(Optional.of("v1"), kv.get("k1"));
        assertEquals(Optional.of("v500"), kv.get("k500"));
        assertEquals(Optional.of("v1000"), kv.get("k1000"));
    }

    @Test
    void testDeleteAfterManyCommands() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("x", "1"), 1);
        kv.apply(Command.set("y", "2"), 2);
        kv.apply(Command.delete("x"), 3);

        assertFalse(kv.get("x").isPresent());
        assertEquals(Optional.of("2"), kv.get("y"));
        assertEquals(1, kv.size());
    }

    @Test
    void testToStringContainsUsefulInformation() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);

        String s = kv.toString();

        assertTrue(s.contains("entries=1"));
        assertTrue(s.contains("lastApplied=1"));
    }

    @Test
    void testApplyUpdatesLastAppliedIndexCorrectly() {
        KVStateMachine kv = new KVStateMachine();

        for (int i = 1; i <= 20; i++) {
            kv.apply(Command.set("k" + i, "v" + i), i);
            assertEquals(i, kv.getLastAppliedIndex());
        }
    }

    @Test
    void testDeleteDoesNotAffectOtherKeys() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("a", "1"), 1);
        kv.apply(Command.set("b", "2"), 2);
        kv.apply(Command.delete("a"), 3);

        assertEquals(Optional.empty(), kv.get("a"));
        assertEquals(Optional.of("2"), kv.get("b"));
    }

    @Test
    void testValueCanBeEmptyString() {
        KVStateMachine kv = new KVStateMachine();

        kv.apply(Command.set("empty", ""), 1);

        assertEquals(Optional.of(""), kv.get("empty"));
    }
}