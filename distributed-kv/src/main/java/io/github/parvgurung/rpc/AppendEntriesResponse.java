package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record AppendEntriesResponse(
    long term,
    boolean success,
    long conflictIndex,
    long conflictTerm
) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("success", success);
        obj.put("conflictIndex", conflictIndex);
        obj.put("conflictTerm", conflictTerm);
        return Json.encode(obj);
    }

    public static AppendEntriesResponse fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new AppendEntriesResponse(
            Json.getLong(obj, "term"),
            Json.getBoolean(obj, "success"),
            Json.getLong(obj, "conflictIndex"),
            Json.getLong(obj, "conflictTerm")
        );
    }
}
