package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record InstallSnapshotResponse(long term) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        return Json.encode(obj);
    }

    public static InstallSnapshotResponse fromJson(String json) {
        return new InstallSnapshotResponse(Json.getLong(Json.decode(json), "term"));
    }
}
