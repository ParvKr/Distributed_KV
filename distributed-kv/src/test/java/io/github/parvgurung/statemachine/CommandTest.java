package io.github.parvgurung.statemachine;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CommandTest {

    @Test
    void testSetFactoryMethod() {
        Command command = Command.set("name", "Alice");

        assertEquals(CommandType.SET, command.type());
        assertEquals("name", command.key());
        assertEquals("Alice", command.value());
    }

    @Test
    void testDeleteFactoryMethod() {
        Command command = Command.delete("name");

        assertEquals(CommandType.DELETE, command.type());
        assertEquals("name", command.key());
        assertNull(command.value());
    }

    @Test
    void testConstructorRejectsNullType() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new Command(null, "key", "value")
        );

        assertTrue(ex.getMessage().contains("Command type"));
    }

    @Test
    void testConstructorRejectsNullKey() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new Command(CommandType.SET, null, "value")
        );

        assertTrue(ex.getMessage().contains("Key"));
    }

    @Test
    void testConstructorRejectsEmptyKey() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new Command(CommandType.SET, "", "value")
        );

        assertTrue(ex.getMessage().contains("Key"));
    }

    @Test
    void testSerializeDeserializeSet() {
        Command original = Command.set("fruit", "apple");

        String serialized = original.serialize();
        Command deserialized = Command.deserialize(serialized);

        assertEquals(original, deserialized);
    }

    @Test
    void testSerializeDeserializeDelete() {
        Command original = Command.delete("fruit");

        String serialized = original.serialize();
        Command deserialized = Command.deserialize(serialized);

        assertEquals(CommandType.DELETE, deserialized.type());
        assertEquals("fruit", deserialized.key());
        assertNull(deserialized.value());
    }

    @Test
    void testPipeEscaping() {
        Command original = Command.set("user|id", "hello|world");

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testBackslashEscaping() {
        Command original = Command.set(
                "C:\\Users\\Admin",
                "D:\\Data\\Files"
        );

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testNewlineEscaping() {
        Command original = Command.set(
                "line1\nline2",
                "value\nanother"
        );

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testMixedEscaping() {
        Command original = Command.set(
                "A|B\\C\nD",
                "X\\Y|Z\n123"
        );

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testEmptyValue() {
        Command original = Command.set("key", "");

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testUnicodeCharacters() {
        Command original = Command.set(
                "こんにちは",
                "안녕하세요 😀"
        );

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testLongStrings() {
        String longString = "x".repeat(10_000);

        Command original = Command.set(longString, longString);

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testMalformedCommandTooFewFields() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Command.deserialize("SET|key")
        );
    }

    @Test
    void testMalformedCommandTooManyFields() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Command.deserialize("SET|a|b|c")
        );
    }

    @Test
    void testInvalidCommandType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Command.deserialize("UPDATE|key|value")
        );
    }

    @Test
    void testTrailingEscapeCharacter() {
        Command original = Command.set("abc\\", "xyz\\");

        Command deserialized = Command.deserialize(original.serialize());

        assertEquals(original, deserialized);
    }

    @Test
    void testEqualsAndHashCode() {
        Command c1 = Command.set("key", "value");
        Command c2 = Command.set("key", "value");

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    void testSerializeIsDeterministic() {
        Command command = Command.set("key", "value");

        assertEquals(command.serialize(), command.serialize());
    }
}