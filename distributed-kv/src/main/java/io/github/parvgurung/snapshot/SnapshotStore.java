package io.github.parvgurung.snapshot;

import io.github.parvgurung.util.FileUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/** Keeps the node's latest snapshot in {@code <dataDir>/snapshot}, replaced atomically. */
public final class SnapshotStore {
    private final Path filePath;

    public SnapshotStore(String dataDir) {
        this.filePath = Path.of(dataDir, "snapshot");
    }

    public void save(Snapshot snapshot) throws IOException {
        Files.createDirectories(filePath.getParent());
        FileUtil.writeAtomically(filePath, snapshot.toJson().getBytes(StandardCharsets.UTF_8));
    }

    public Optional<Snapshot> load() throws IOException {
        if (!Files.exists(filePath))
            return Optional.empty();
        try {
            return Optional.of(Snapshot.fromJson(Files.readString(filePath, StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Snapshot file " + filePath + " is corrupted: " + e.getMessage(), e);
        }
    }
}
