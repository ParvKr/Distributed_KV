package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void testConstructorCreatesValidEntry() {
        Command command = Command.set("name", "Alice");

        LogEntry entry = new LogEntry(1, 1, command);

        assertEquals(1, entry.index());
        assertEquals(1, entry.term());
        assertEquals(command, entry.command());
    }

    @Test
    void testConstructorRejectsNegativeIndex() {
        Command command = Command.set("k", "v");

        assertThrows(
                IllegalArgumentException.class,
                () -> new LogEntry(-1, 1, command)
        );
    }

    @Test
    void testConstructorRejectsNegativeTerm() {
        Command command = Command.set("k", "v");

        assertThrows(
                IllegalArgumentException.class,
                () -> new LogEntry(1, -1, command)
        );
    }

    @Test
    void testConstructorRejectsNullCommand() {
        assertThrows(
                NullPointerException.class,
                () -> new LogEntry(1, 1, null)
        );
    }

    @Test
    void testSerializeDeserializeRoundTrip() {
        LogEntry original = new LogEntry(
                10,
                7,
                Command.set("username", "parv")
        );

        LogEntry restored = LogEntry.deserialize(original.serialize());

        assertEquals(original, restored);
    }

    @Test
    void testSerializeDeleteCommand() {
        LogEntry original = new LogEntry(
                5,
                3,
                Command.delete("temp")
        );

        LogEntry restored = LogEntry.deserialize(original.serialize());

        assertEquals(original, restored);
    }

    @Test
    void testSerializeWithSpecialCharacters() {
        LogEntry original = new LogEntry(
                2,
                9,
                Command.set(
                        "A|B\\C\nD",
                        "X\\Y|Z\n123"
                )
        );

        LogEntry restored = LogEntry.deserialize(original.serialize());

        assertEquals(original, restored);
    }

    @Test
    void testUnicodeSerialization() {
        LogEntry original = new LogEntry(
                3,
                2,
                Command.set(
                        "こんにちは",
                        "안녕하세요 😀"
                )
        );

        LogEntry restored = LogEntry.deserialize(original.serialize());

        assertEquals(original, restored);
    }

    @Test
    void testLargeStrings() {
        String large = "x".repeat(10000);

        LogEntry original = new LogEntry(
                100,
                200,
                Command.set(large, large)
        );

        LogEntry restored = LogEntry.deserialize(original.serialize());

        assertEquals(original, restored);
    }

    @Test
    void testMalformedEntryTooFewFields() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LogEntry.deserialize("1 | 2")
        );
    }

    @Test
    void testMalformedEntryTooManySeparators() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LogEntry.deserialize("1 | 2")
        );
    }

    @Test
    void testInvalidIndex() {
        assertThrows(
                NumberFormatException.class,
                () -> LogEntry.deserialize("abc | 2 | SET|x|1")
        );
    }

    @Test
    void testInvalidTerm() {
        assertThrows(
                NumberFormatException.class,
                () -> LogEntry.deserialize("1 | xyz | SET|x|1")
        );
    }

    @Test
    void testInvalidCommand() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LogEntry.deserialize("1 | 2 | UPDATE|x|1")
        );
    }

    @Test
    void testEqualsAndHashCode() {
        LogEntry e1 = new LogEntry(
                1,
                1,
                Command.set("a", "b")
        );

        LogEntry e2 = new LogEntry(
                1,
                1,
                Command.set("a", "b")
        );

        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
    }

    @Test
    void testSerializeIsDeterministic() {
        LogEntry entry = new LogEntry(
                7,
                8,
                Command.set("key", "value")
        );

        assertEquals(entry.serialize(), entry.serialize());
    }

    @Test
    void testToStringContainsUsefulInformation() {
        LogEntry entry = new LogEntry(
                4,
                5,
                Command.set("x", "10")
        );

        String s = entry.toString();

        assertTrue(s.contains("index=4"));
        assertTrue(s.contains("term=5"));
        assertTrue(s.contains("command"));
    }

    @Test
    void testRejectZeroIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LogEntry(0, 1, Command.set("k", "v"))
        );
    }

    @Test
    void testRejectZeroTerm() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LogEntry(1, 0, Command.set("k", "v"))
        );
    }
}