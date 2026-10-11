package io.github.parvgurung.statemachine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Keys and values are persisted as UTF-8, so they must be text that survives that round trip. */
class CommandTextValidationTest {
    private static final String LONE_HIGH = "ab\uD83Dcd";
    private static final String LONE_LOW = "ab\uDE00cd";
    private static final String REVERSED_PAIR = "\uDE00\uD83D";
    private static final String TRAILING_HIGH = "abc\uD83D";

    @Test
    void unpairedSurrogatesAreRejectedInKeysValuesAndClientIds() {
        for (String bad : new String[]{LONE_HIGH, LONE_LOW, REVERSED_PAIR, TRAILING_HIGH}) {
            assertThrows(IllegalArgumentException.class, () -> Command.set(bad, "v"), "key " + bad.length());
            assertThrows(IllegalArgumentException.class, () -> Command.set("k", bad), "value " + bad.length());
            assertThrows(IllegalArgumentException.class, () -> Command.delete(bad), "delete key");
            assertThrows(IllegalArgumentException.class, () -> Command.set("k", "v", bad, 1), "client id");
        }
    }

    @Test
    void properlyPairedEmojiAndOtherUnicodeAreAccepted() {
        String text = "😀 日本語 é 🚀🚀";
        Command c = Command.set(text, text, text, 1);
        assertEquals(c, Command.deserialize(c.serialize()));
    }
}
