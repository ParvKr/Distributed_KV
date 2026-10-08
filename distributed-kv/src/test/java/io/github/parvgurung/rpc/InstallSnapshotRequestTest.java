package io.github.parvgurung.rpc;

import io.github.parvgurung.snapshot.Snapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InstallSnapshotRequestTest {
    @Test
    void requestRoundTripsThroughJson() {
        Snapshot snap = new Snapshot(12, 3, Map.of("k", "v \"quoted\" é", "empty", ""), Map.of("A", 4L));
        InstallSnapshotRequest req = new InstallSnapshotRequest(5, 2, snap);
        assertEquals(req, InstallSnapshotRequest.fromJson(req.toJson()));
    }

    @Test
    void responseRoundTripsThroughJson() {
        InstallSnapshotResponse resp = new InstallSnapshotResponse(9);
        assertEquals(resp, InstallSnapshotResponse.fromJson(resp.toJson()));
    }
}
