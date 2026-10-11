package io.github.parvgurung.fuzz;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/** Random-data helpers shared by the fuzz tests. Every test prints/uses its seed so failures reproduce. */
final class Gen {
    private Gen() {}

    // Things that break naive serializers: delimiters, escapes, controls, line breaks, non-BMP, etc.
    // Whole code points, so an emoji is never split into two lone surrogate halves.
    private static final String[] NASTY = {
        "|", "\\", "/", "\"", "'", "\n", "\r", "\t", "\b", "\f", "\u0000", "\u0001", "\u001f", "\u007f", " ",
        "\u2028", "\u2029", "\u00e9", "\u3053", "\u3093", "\u0645", "\uD83D\uDE00", "p", "n", "u", "{", "}", "[", "]", ":", ",", "%"
    };

    /** Well-formed text: every surrogate is part of a valid pair. */
    static String string(Random r, int maxLen) {
        return string(r, maxLen, false);
    }

    /** Like {@link #string} but may contain unpaired surrogates (valid in JSON and Java, invalid as UTF-8 text). */
    static String rawString(Random r, int maxLen) {
        return string(r, maxLen, true);
    }

    private static String string(Random r, int maxLen, boolean allowLoneSurrogates) {
        int len = r.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            switch (r.nextInt(allowLoneSurrogates ? 5 : 4)) {
                case 4 -> sb.append((char) (0xD800 + r.nextInt(0x800)));          // a lone surrogate
                case 0 -> sb.append(NASTY[r.nextInt(NASTY.length)]);
                case 1 -> sb.append((char) (0x20 + r.nextInt(0x5f)));              // printable ASCII
                case 2 -> sb.append((char) r.nextInt(0xD800));                      // any BMP char below surrogates
                default -> sb.append(Character.toChars(0x10000 + r.nextInt(0x1000))); // astral (surrogate pair)
            }
        }
        return sb.toString();
    }

    static String nonEmptyString(Random r, int maxLen) {
        String s = string(r, maxLen);
        return s.isEmpty() ? "k" : s;
    }

    /** A random JSON-compatible value tree (maps, lists, strings, ints/longs, booleans, null). */
    static Object jsonValue(Random r, int depth) {
        int kind = r.nextInt(depth <= 0 ? 5 : 7);
        return switch (kind) {
            case 0 -> null;
            case 1 -> r.nextBoolean();
            case 2 -> r.nextInt();                                   // decodes as Integer
            case 3 -> (long) Integer.MAX_VALUE + 1 + r.nextInt(1000);   // decodes as Long
            case 4 -> rawString(r, 12);
            case 5 -> {
                List<Object> list = new ArrayList<>();
                for (int i = r.nextInt(4); i > 0; i--) list.add(jsonValue(r, depth - 1));
                yield list;
            }
            default -> jsonObject(r, depth - 1);
        };
    }

    static Map<String, Object> jsonObject(Random r, int depth) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = r.nextInt(5); i > 0; i--) map.put(rawString(r, 8), jsonValue(r, depth));
        return map;
    }
}
