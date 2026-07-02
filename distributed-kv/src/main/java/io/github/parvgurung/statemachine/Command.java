package io.github.parvgurung.statemachine;

public record Command(CommandType type, String key, String value) {

    public Command {
        if (type == null) {
            throw new IllegalArgumentException("Command type cannot be null");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    public static Command set(String key, String value) {
        return new Command(CommandType.SET, key, value);
    }

    public static Command delete(String key) {
        return new Command(CommandType.DELETE, key, null);
    }

    public String serialize() {
        return String.join("|", type.name(), escape(key), escape(value));
    }

    public static Command deserialize(String line) {
        var parts = line.split("\\|", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Malformed command line: " + line);
        }
        CommandType type = CommandType.valueOf(parts[0]);
        String key = unescape(parts[1]);
        String value = unescape(parts[2]);
        return new Command(type, key, type == CommandType.SET ? value : null);
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
