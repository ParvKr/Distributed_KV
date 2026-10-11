package io.github.parvgurung.fuzz;

import io.github.parvgurung.util.Json;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class JsonFuzzTest {
    @Test
    void randomValueTreesRoundTrip() {
        for (long seed = 1; seed <= 400; seed++) {
            Map<String, Object> original = Gen.jsonObject(new Random(seed), 3);
            String json = Json.encode(original);
            assertEquals(original, Json.decode(json), "seed " + seed + " json=" + json);
        }
    }

    @Test
    void encodedOutputIsPureAsciiWithNoRawControlCharacters() {
        // The log and RPC layers split on newlines, so a raw one inside a string would be catastrophic.
        for (long seed = 1; seed <= 200; seed++) {
            String json = Json.encode(Gen.jsonObject(new Random(seed), 3));
            for (char c : json.toCharArray())
                assertTrue(c >= 0x20 && c <= 0x7e, "seed " + seed + " produced raw char U+" + Integer.toHexString(c));
        }
    }

    @Test
    void arbitraryGarbageOnlyEverFailsWithIllegalArgumentException() {
        String alphabet = "{}[]\":,\\ntruefalsenul0123456789-+.eE \n\t\u0000x/\u00e9";
        Random r = new Random(42);
        for (int i = 0; i < 20000; i++) {
            StringBuilder sb = new StringBuilder();
            for (int n = r.nextInt(40); n > 0; n--) sb.append(alphabet.charAt(r.nextInt(alphabet.length())));
            String input = sb.toString();
            try {
                Json.decode(input);
            } catch (IllegalArgumentException expected) {
                // fine: rejected cleanly
            } catch (Throwable t) {
                fail("input [" + input.replace("\n", "\\n") + "] threw " + t);
            }
        }
    }

    @Test
    void mutatedValidJsonOnlyEverFailsCleanly() {
        Random r = new Random(7);
        for (int i = 0; i < 5000; i++) {
            String valid = Json.encode(Gen.jsonObject(new Random(i), 3));
            if (valid.length() < 2) continue;
            StringBuilder mutated = new StringBuilder(valid);
            for (int m = 1 + r.nextInt(3); m > 0; m--) {
                int pos = r.nextInt(mutated.length());
                switch (r.nextInt(3)) {
                    case 0 -> mutated.deleteCharAt(pos);
                    case 1 -> mutated.insert(pos, "{}[]\":,\\".charAt(r.nextInt(8)));
                    default -> mutated.setCharAt(pos, (char) (32 + r.nextInt(90)));
                }
                if (mutated.length() == 0) break;
            }
            try {
                Json.decode(mutated.toString());
            } catch (IllegalArgumentException expected) {
            } catch (Throwable t) {
                fail("input [" + mutated + "] threw " + t);
            }
        }
    }

    @Test
    void absurdlyDeepNestingIsRejectedInsteadOfOverflowingTheStack() {
        // A hostile peer or client can send this to any endpoint.
        String deepArray = "{\"a\":" + "[".repeat(200_000) + "]".repeat(200_000) + "}";
        String deepObject = "{\"a\":".repeat(200_000) + "1" + "}".repeat(200_000);
        for (String input : new String[]{deepArray, deepObject}) {
            try {
                Json.decode(input);
            } catch (IllegalArgumentException expected) {
                // good
            } catch (Throwable t) {
                fail("deeply nested input threw " + t.getClass().getName());
            }
        }
    }

    @Test
    void numbersAtTheEdgesOfTheirRanges() {
        Map<String, Object> m = Json.decode("{\"a\":2147483647,\"b\":2147483648,\"c\":-2147483648,\"d\":-2147483649,\"e\":9223372036854775807}");
        assertEquals(Integer.MAX_VALUE, m.get("a"));
        assertEquals(2147483648L, m.get("b"));
        assertEquals(Integer.MIN_VALUE, m.get("c"));
        assertEquals(-2147483649L, m.get("d"));
        assertEquals(Long.MAX_VALUE, m.get("e"));
        assertThrows(IllegalArgumentException.class, () -> Json.decode("{\"a\":9223372036854775808}"));
    }
}
