package io.github.parvgurung.rpc;

import io.github.parvgurung.snapshot.Snapshot;
import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record InstallSnapshotRequest(
    long term,
    long leaderId,
    Snapshot snapshot
) {
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("term", term);
        obj.put("leaderId", leaderId);
        obj.put("snapshot", snapshot.toMap());
        return Json.encode(obj);
    }

    public static InstallSnapshotRequest fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new InstallSnapshotRequest(
            Json.getNonNegativeLong(obj, "term"),
            Json.getNodeId(obj, "leaderId"),
            Snapshot.fromMap(Json.getMap(obj, "snapshot"))
        );
    }
}
