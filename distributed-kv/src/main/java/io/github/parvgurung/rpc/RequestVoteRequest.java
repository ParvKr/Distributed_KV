package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record RequestVoteRequest(
    long term,
    long candidateId,
    long lastLogIndex,
    long lastLogTerm
) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("candidateId", candidateId);
        obj.put("lastLogIndex", lastLogIndex);
        obj.put("lastLogTerm", lastLogTerm);
        return Json.encode(obj);
    }

    public static RequestVoteRequest fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new RequestVoteRequest(
            Json.getLong(obj, "term"),
            Json.getLong(obj, "candidateId"),
            Json.getLong(obj, "lastLogIndex"),
            Json.getLong(obj, "lastLogTerm")
        );
    }
}
