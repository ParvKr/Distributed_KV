package io.github.parvgurung.rpc;

import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.util.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record AppendEntriesRequest(
    long term,
    long leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("leaderId", leaderId);
        obj.put("prevLogIndex", prevLogIndex);
        obj.put("prevLogTerm", prevLogTerm);
        obj.put("leaderCommit", leaderCommit);
        List<Object> entriesObjs = new ArrayList<>();
        for(LogEntry e: entries) {
            Map<String, Object> eoMap = new LinkedHashMap<>();
            eoMap.put("index", e.index());
            eoMap.put("term", e.term());
            eoMap.put("commandType", e.command().type().name());
            eoMap.put("key", e.command().key());
            eoMap.put("value", e.command().value());
            entriesObjs.add(eoMap);
        }
        obj.put("entries", entriesObjs);
        return Json.encode(obj);
    }

    public static AppendEntriesRequest fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        List<LogEntry> entries = new ArrayList<>();
        for(Map<String, Object> eoMap: Json.getMapList(obj, "entries")) {
            long index = Json.getLong(eoMap, "index");
            long term = Json.getLong(eoMap, "term");
            Command command = new Command(
                io.github.parvgurung.statemachine.CommandType.valueOf(Json.getString(eoMap, "commandType")),
                Json.getString(eoMap, "key"),
                Json.getString(eoMap, "value")
            );
            entries.add(new LogEntry(index, term, command));
        }
        return new AppendEntriesRequest(
            Json.getLong(obj, "term"),
            Json.getLong(obj, "leaderId"),
            Json.getLong(obj, "prevLogIndex"),
            Json.getLong(obj, "prevLogTerm"),
            entries,
            Json.getLong(obj, "leaderCommit")
        );
    }
}