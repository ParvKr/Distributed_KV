package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record RequestVoteRequest(
    long term,
    long candidateId,
    long lastLogIndex,
    long lastLogTerm,
    boolean preVote
) {
    public RequestVoteRequest(long term, long candidateId, long lastLogIndex, long lastLogTerm) {
        this(term, candidateId, lastLogIndex, lastLogTerm, false);
    }

    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("candidateId", candidateId);
        obj.put("lastLogIndex", lastLogIndex);
        obj.put("lastLogTerm", lastLogTerm);
        obj.put("preVote", preVote);
        return Json.encode(obj);
    }

    public static RequestVoteRequest fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new RequestVoteRequest(
            Json.getNonNegativeLong(obj, "term"),
            Json.getNodeId(obj, "candidateId"),
            Json.getNonNegativeLong(obj, "lastLogIndex"),
            Json.getNonNegativeLong(obj, "lastLogTerm"),
            obj.get("preVote") != null && Json.getBoolean(obj, "preVote")   // absent from older peers: a real vote
        );
    }
}
