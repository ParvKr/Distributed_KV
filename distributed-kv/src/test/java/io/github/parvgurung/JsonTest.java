package io.github.parvgurung;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonTest {

    public static void main(String[] args) {

        Map<String, Object> original = new LinkedHashMap<>();

        original.put("term", 5);
        original.put("leaderId", 1);
        original.put("success", true);
        original.put("message", "Hello\nWorld");
        original.put("numbers", List.of(1, 2, 3));
        original.put("array", List.of());
        original.put("leader", null);
        original.put("jp", "こんにちは");

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("index", 10);
        entry.put("command", "SET");

        original.put("entry", entry);

        String json = Json.encode(original);

        System.out.println("Encoded:");
        System.out.println(json);

        Map<String, Object> decoded = Json.decode(json);
        Map<String, Object> empty = new LinkedHashMap<>();

        System.out.println("\nDecoded:");
        System.out.println(decoded);

        assert Json.getInt(decoded, "term") == 5;
        assert Json.getInt(decoded, "leaderId") == 1;
        assert Json.getBoolean(decoded, "success");
        assert Json.getString(decoded, "message").equals("Hello\nWorld");
        assert Json.getString(decoded, "jp").equals("こんにちは");
        assert Json.decode("{\"term\":5}").equals(original);
        assert Json.decode("{}").equals(empty);

        System.out.println("\nAll tests passed.");
    }
}