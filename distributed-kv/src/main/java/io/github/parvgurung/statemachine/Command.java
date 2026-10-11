package io.github.parvgurung.statemachine;

/**
 * A state machine command. Writes issued by a client carry that client's ID and a sequence number so
 * that a retried request is applied at most once; commands without them (clientId == null, seq == 0)
 * are always applied.
 */
public record Command(CommandType type, String key, String value, String clientId, long seq) {

    public Command(CommandType type, String key, String value) {
        this(type, key, value, null, 0);
    }

    public Command {
        if (type == null) {
            throw new IllegalArgumentException("Command type cannot be null");
        }
        requireWellFormed("Key", key);
        requireWellFormed("Value", value);
        requireWellFormed("Client ID", clientId);
        if (clientId == null ? seq != 0 : (clientId.isEmpty() || seq < 1)) {
            throw new IllegalArgumentException("A client ID needs a sequence number >= 1, and neither is allowed without the other");
        }
        if (type == CommandType.NOOP) {
            key = "";
            value = null;
            clientId = null;
            seq = 0;
        } else {
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key cannot be null or empty");
            }
            if (type == CommandType.SET && value == null) {
                throw new IllegalArgumentException("Value cannot be null for SET");
            }
        }
    }

    // Keys and values are stored as UTF-8. An unpaired surrogate cannot be encoded: it would be written as
    // '?', so after a restart the data would differ from what was acknowledged (and distinct keys could collide).
    private static void requireWellFormed(String what, String text) {
        if (text == null) return;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                i++;
            } else if (Character.isSurrogate(c)) {
                throw new IllegalArgumentException(what + " contains an unpaired surrogate character and is not valid text");
            }
        }
    }

    /** Placeholder a new leader appends so entries from earlier terms can be committed. */
    public static Command noop() {
        return new Command(CommandType.NOOP, "", null);
    }

    public static Command set(String key, String value) {
        return new Command(CommandType.SET, key, value);
    }

    public static Command set(String key, String value, String clientId, long seq) {
        return new Command(CommandType.SET, key, value, clientId, seq);
    }

    public static Command delete(String key) {
        return new Command(CommandType.DELETE, key, null);
    }

    public static Command delete(String key, String clientId, long seq) {
        return new Command(CommandType.DELETE, key, null, clientId, seq);
    }

    /** "TYPE|key|value", with "|clientId|seq" appended when the command belongs to a client session. */
    public String serialize() {
        String base = String.join("|", type.name(), escape(key), escape(value));
        return clientId == null ? base : base + "|" + escape(clientId) + "|" + seq;
    }

    public static Command deserialize(String line) {
        var parts = line.split("\\|", -1);
        if (parts.length != 3 && parts.length != 5) {
            throw new IllegalArgumentException("Malformed command line: " + line);
        }
        CommandType type = CommandType.valueOf(parts[0]);
        String key = unescape(parts[1]);
        String value = unescape(parts[2]);
        String clientId = parts.length == 5 ? unescape(parts[3]) : null;
        long seq = parts.length == 5 ? Long.parseLong(parts[4]) : 0;
        return new Command(type, key, type == CommandType.SET ? value : null, clientId, seq);
    }

    private static String escape(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\").replace("|", "\\p").replace("\n", "\\n");
    }

    private static String unescape(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\\' && i + 1 < str.length()) {
                char next = str.charAt(++i);
                switch (next) {
                    case 'p'-> sb.append('|');
                    case 'n'-> sb.append('\n');
                    case '\\'-> sb.append('\\');
                    default -> {
                        sb.append(c);
                        sb.append(next);
                    }
                } 
            }
            else
                sb.append(c);
        }
        return sb.toString();
    }
}
