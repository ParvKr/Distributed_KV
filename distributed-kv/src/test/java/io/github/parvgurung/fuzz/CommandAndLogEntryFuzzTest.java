package io.github.parvgurung.fuzz;

import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class CommandAndLogEntryFuzzTest {
    private static Command randomCommand(Random r) {
        String key = Gen.nonEmptyString(r, 12);
        boolean session = r.nextBoolean();
        String clientId = session ? Gen.nonEmptyString(r, 8) : null;
        long seq = session ? 1 + r.nextInt(1_000_000) : 0;
        return switch (r.nextInt(3)) {
            case 0 -> Command.set(key, Gen.string(r, 20), clientId, seq);
            case 1 -> Command.delete(key, clientId, seq);
            default -> Command.noop();
        };
    }

    @Test
    void commandsRoundTripThroughTheirTextForm() {
        for (long seed = 1; seed <= 3000; seed++) {
            Command c = randomCommand(new Random(seed));
            Command back = Command.deserialize(c.serialize());
            assertEquals(c, back, "seed " + seed + " serialized=" + c.serialize());
        }
    }

    @Test
    void serializedCommandsNeverContainLineBreaks() {
        // The log is one entry per line, split on '\n'.
        for (long seed = 1; seed <= 3000; seed++) {
            String text = randomCommand(new Random(seed)).serialize();
            assertFalse(text.contains("\n"), "seed " + seed);
        }
    }

    @Test
    void logEntriesRoundTripEvenWhenValuesLookLikeTheirOwnSeparators() {
        Random r = new Random(99);
        for (int i = 0; i < 3000; i++) {
            Command c = randomCommand(new Random(i));
            LogEntry e = new LogEntry(1 + r.nextInt(1000), 1 + r.nextInt(50), c);
            String line = e.serialize();
            assertFalse(line.contains("\n"), "entry " + i);
            assertEquals(e, LogEntry.deserialize(line), "entry " + i + " line=" + line);
        }
    }

    @Test
    void valuesContainingTheLogSeparatorSurvive() {
        Command c = Command.set("k", " | 1 | 2 | SET|x|y ");
        LogEntry e = new LogEntry(7, 3, c);
        assertEquals(e, LogEntry.deserialize(e.serialize()));
    }

    @Test
    void emptyAndWhitespaceValuesSurvive() {
        for (String v : new String[]{"", " ", "  ", "\t", "\r", "\r\n"}) {
            Command c = Command.set("k", v, "client", 3);
            assertEquals(c, Command.deserialize(c.serialize()), "value [" + v.replace("\r", "\\r").replace("\n", "\\n") + "]");
        }
    }

    @Test
    void garbageNeverCrashesTheParserWithAnUnexpectedException() {
        Random r = new Random(5);
        for (int i = 0; i < 5000; i++) {
            String line = Gen.string(r, 30);
            try {
                Command.deserialize(line);
            } catch (IllegalArgumentException expected) {
            } catch (Throwable t) {
                fail("command line [" + line + "] threw " + t);
            }
            try {
                LogEntry.deserialize(line);
            } catch (IllegalArgumentException expected) {
            } catch (Throwable t) {
                fail("log line [" + line + "] threw " + t);
            }
        }
    }
}
