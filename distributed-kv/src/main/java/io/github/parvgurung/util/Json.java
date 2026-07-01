package io.github.parvgurung.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Json {
    private Json() {}

    // ENCODING METHODS

    public static String encode(Map<?, ?> obj) {
        StringBuilder sb = new StringBuilder();
        encodeObject(obj, sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void encodeValue(Object value, StringBuilder sb) {
        if(value == null)
            sb.append("null");
        else if(value instanceof String s)
            encodeString(s, sb);
        else if(value instanceof Boolean || value instanceof Number)
            sb.append(value);
        else if(value instanceof Map)
            encodeObject((Map<?, ?>) value, sb);
        else if(value instanceof List)
            encodeArray((List<?>) value, sb);
        else
            throw new IllegalArgumentException("Unsupported JSON value type: " + value.getClass());
    }

    private static void encodeObject(Map<?, ?> obj, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for(Map.Entry<?, ?> e: obj.entrySet()) {
            if(!first) sb.append(',');
            first = false;
            Object key = e.getKey();
            if(!(key instanceof String s))
                throw new IllegalArgumentException("JSON object keys must be strings, but got: " + key.getClass());
            encodeString(s, sb);
            sb.append(':');
            encodeValue(e.getValue(), sb);
        }
        sb.append('}');
    }

    private static void encodeArray(List<?> list, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for(Object item: list) {
            if(!first) sb.append(',');
            first = false;
            encodeValue(item, sb);
        }
        sb.append(']');
    }

    private static void encodeString(String s, StringBuilder sb) {
        sb.append('"');
        for(int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch(c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if(c < 0x20 || c > 0x7E) {
                        sb.append(String.format("\\u%04x", (int)c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    // Additional helper methods since decode() returns raw Object types, we need to provide methods to extract specific types from the parsed Map.
        
    public static int getInt(Map<?, ?> obj, String key) {
        Object value = obj.get(key);
        if(value instanceof Integer i)
            return i;
        if(value instanceof Long l)
            return l.intValue();
        throw new IllegalArgumentException("Field '" + key + "' is not an int: " + value);
    }

    public static long getLong(Map<?, ?> obj, String key) {
        Object value = obj.get(key);
        if(value instanceof Integer i)
            return i;
        if(value instanceof Long l)
            return l;
        throw new IllegalArgumentException("Field '" + key + "' is not a long: " + value);
    }

    public static String getString(Map<?, ?> obj, String key) {
        Object value = obj.get(key);
        if(value instanceof String s)
            return s;
        if(value == null)
            return null;
        throw new IllegalArgumentException("Field '" + key + "' is not a string: " + value);
    }

    public static boolean getBoolean(Map<?, ?> obj, String key) {
        Object value = obj.get(key);
        if(value instanceof Boolean b)
            return b;
        throw new IllegalArgumentException("Field '" + key + "' is not a boolean: " + value);
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getMapList(Map<String, Object> obj, String key) {
        Object value = obj.get(key);
        if(value == null)
            return new ArrayList<>();
        return (List<Map<String, Object>>) (List<?>) value;
    }

    // DECODING METHODS

    public static Map<String, Object> decode(String json) {
        Parser p = new Parser(json);
        p.skipWhitespace();
        Object result = p.parseValue();
        if(!(result instanceof Map))
            throw new IllegalArgumentException("Expected JSON object at the root, but got: " +result.getClass());
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        return map;
    }

    private static final class Parser {
        private final String s;
        private int pos = 0;

        Parser(String s) {
            this.s = s;
        }

        void skipWhitespace() {
            while(pos < s.length() && Character.isWhitespace(s.charAt(pos)))
                pos++;
        }

        char peek() {
            if(pos >= s.length())
                throw new IllegalArgumentException("Unexpected end of input at position " + pos + " in: " + s);
            return s.charAt(pos);
        }

        void expect(char c) {
            if(pos >= s.length() || s.charAt(pos) != c)
                throw new IllegalArgumentException("Expected '" + c + "' at position " + pos + " in: " + s);
            pos++;
        }

        Object parseValue() {
            skipWhitespace();
            char c = peek();
            return switch(c) {
                case '"' -> parseString();
                case '{' -> parseObject();
                case '[' -> parseArray();
                case 't', 'f' -> parseBoolean();
                case 'n' -> parseNull();
                default -> {
                    if((c >= '0' && c <= '9') || c == '-')
                        yield parseNumber();
                    else
                        throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + pos + " in: " + s);
                }
            };
        }

        Map<String, Object> parseObject() {
            Map<String, Object> result = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if(peek() == '}') {
                pos++;
                return result;
            }
            while(true) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                Object value = parseValue();
                result.put(key, value);
                skipWhitespace();
                if(peek() == ',') {
                    pos++;
                    continue;
                }
                expect('}');
                break;
            }
            return result;
        }

        List<Object> parseArray() {
            List<Object> result = new ArrayList<>();
            expect('[');
            skipWhitespace();
            if(peek() == ']') {
                pos++;
                return result;
            }
            while(true) {
                skipWhitespace();
                Object value = parseValue();
                result.add(value);
                skipWhitespace();
                if(peek() == ',') {
                    pos++;
                    continue;
                }
                expect(']');
                break;
            }
            return result;
        }

        String parseString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while(peek() != '"') {
                if(pos >= s.length())
                    throw new IllegalArgumentException("Unterminated string at position " + pos + " in: " + s);
                char c = s.charAt(pos++);
                if(c == '\\') {
                    if(pos >= s.length())
                        throw new IllegalArgumentException("Unterminated escape sequence at position " + pos + " in: " + s);
                    c = s.charAt(pos++);
                    switch(c) {
                        case '"', '\\', '/' -> sb.append(c);
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if(pos + 4 > s.length())
                                throw new IllegalArgumentException("Invalid unicode escape sequence at position " + pos + " in: " + s);
                            String hex = s.substring(pos, pos + 4);
                            try {
                                int codePoint = Integer.parseInt(hex, 16);
                                sb.append((char) codePoint);
                            } catch(NumberFormatException e) {
                                throw new IllegalArgumentException("Invalid unicode escape sequence: \\u" + hex + " at position " + pos + " in: " + s);
                            }
                            pos += 4;
                        }
                        default -> throw new IllegalArgumentException("Invalid escape character '\\" + c + "' at position " + pos + " in: " + s);
                    }
                } else {
                    sb.append(c);
                }
            }
            expect('"');
            return sb.toString();
        }

        Boolean parseBoolean() {
            if(s.startsWith("true", pos)) {
                pos += 4;
                return true;
            } else if(s.startsWith("false", pos)) {
                pos += 5;
                return false;
            } else {
                throw new IllegalArgumentException("Expected 'true' or 'false' at position " + pos + " in: " + s);
            }
        }

        Object parseNull() {
            if(s.startsWith("null", pos)) {
                pos += 4;
                return null;
            } else {
                throw new IllegalArgumentException("Expected 'null' at position " + pos + " in: " + s);
            }
        }

        Number parseNumber() {
            int start = pos;
            if(peek() == '-') pos++;
            while(pos < s.length() && Character.isDigit(s.charAt(pos))) pos++;
            if(pos < s.length() && s.charAt(pos) == '.') {
                pos++;
                while(pos < s.length() && Character.isDigit(s.charAt(pos))) pos++;
            }
            if(pos < s.length() && (s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                pos++;
                if(pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-')) pos++;
                while(pos < s.length() && Character.isDigit(s.charAt(pos))) pos++;
            }
            String numberStr = s.substring(start, pos);
            try {
                if(numberStr.contains(".") || numberStr.contains("e") || numberStr.contains("E")) {
                    return Double.parseDouble(numberStr);
                } else {
                    long longValue = Long.parseLong(numberStr);
                    if(longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE)
                        return (int) longValue;
                    else
                        return longValue;
                }
            } catch(NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number format: " + numberStr + " at position " + start + " in: " + s);
            }
        }
    }
}