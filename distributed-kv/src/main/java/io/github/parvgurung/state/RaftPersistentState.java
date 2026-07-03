package io.github.parvgurung.state;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class RaftPersistentState {
    private final Path filePath;
    private long currentTerm = 0;
    private Integer votedFor = null;

    public RaftPersistentState(String dataDir) {
        this.filePath = Path.of(dataDir, "raft.state");
    }

    public synchronized void init() throws IOException {
        Files.createDirectories(filePath.getParent());
        if (Files.exists(filePath)) {
            for (String line : Files.readAllLines(filePath, StandardCharsets.UTF_8)) {
                if (line.startsWith("term=")) {
                    currentTerm = Long.parseLong(line.substring("term=".length()));
                    if(currentTerm < 0)
                        throw new IllegalStateException("Corrupted state file: negative term " + currentTerm);
                } else if (line.startsWith("votedFor=")) {
                    String v = line.substring("votedFor=".length());
                    votedFor = v.isBlank() ? null : Integer.parseInt(v);
                }
            }
        } else {
            persist();
        }
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized Integer votedFor() {
        return votedFor;
    }

    public synchronized void setCurrentTerm(long term) {
        if (term < currentTerm)
            throw new IllegalArgumentException("Cannot move term backward: " + currentTerm + " -> " + term);
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        persist();
    }

    public synchronized void setVotedFor(Integer candidateId) {
        this.votedFor = candidateId;
        persist();
    }

    private void persist() {
        try {
            String content = "term=" + currentTerm + "\nvotedFor=" + (votedFor == null ? "" : votedFor) + "\n";
            Files.writeString(filePath, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist Raft state -- cannot safely continue", e);
        }
    }

    @Override
    public synchronized String toString() {
        return "RaftPersistentState{term=" + currentTerm + ", votedFor=" + votedFor + "}";
    }
}