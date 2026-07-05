package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record RequestVoteResponse (
    long term,
    boolean voteGranted
) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("voteGranted", voteGranted);
        return Json.encode(obj);
    }

    public static RequestVoteResponse fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new RequestVoteResponse(
            Json.getLong(obj, "term"),
            Json.getBoolean(obj, "voteGranted")
        );
    }
}
