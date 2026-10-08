package io.github.parvgurung.snapshot;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything needed to stand in for the log prefix {@code 1..lastIncludedIndex}: the key/value data and
 * the per-client sequence numbers used to drop retried requests. The same JSON is stored on disk and sent
 * in InstallSnapshot.
 */
public record Snapshot(
    long lastIncludedIndex,
    long lastIncludedTerm,
    Map<String, String> data,
    Map<String, Long> sessions
) {
    public Snapshot {
        if (lastIncludedIndex < 1) throw new IllegalArgumentException("lastIncludedIndex must be >= 1");
        if (lastIncludedTerm < 1) throw new IllegalArgumentException("lastIncludedTerm must be >= 1");
        data = Map.copyOf(data);
        sessions = Map.copyOf(sessions);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("lastIncludedIndex", lastIncludedIndex);
        obj.put("lastIncludedTerm", lastIncludedTerm);
        obj.put("data", data);
        obj.put("sessions", sessions);
        return obj;
    }

    public String toJson() {
        return Json.encode(toMap());
    }

    public static Snapshot fromMap(Map<String, Object> obj) {
        Map<String, String> data = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : Json.getMap(obj, "data").entrySet()) {
            if (!(e.getValue() instanceof String value))
                throw new IllegalArgumentException("Snapshot value for key '" + e.getKey() + "' is not a string");
            data.put(e.getKey(), value);
        }
        Map<String, Long> sessions = new LinkedHashMap<>();
        Map<String, Object> rawSessions = Json.getMap(obj, "sessions");
        for (String clientId : rawSessions.keySet()) {
            sessions.put(clientId, Json.getLong(rawSessions, clientId));
        }
        return new Snapshot(Json.getLong(obj, "lastIncludedIndex"), Json.getLong(obj, "lastIncludedTerm"), data, sessions);
    }

    public static Snapshot fromJson(String json) {
        return fromMap(Json.decode(json));
    }
}
