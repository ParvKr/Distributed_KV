package io.github.parvgurung.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class PersistentLog implements AutoCloseable {
    private final Path filePath;
    private final List<LogEntry> entries = new ArrayList<>();
    private RandomAccessFile raf;
    private FileChannel channel;
    public PersistentLog(String dataDir) {
        this.filePath = Path.of(dataDir, "raft.log");
    }

    public synchronized void init() throws IOException {
        if(raf != null || channel != null)
            throw new IllegalStateException("PersistentLog is already initialized");
        Files.createDirectories(filePath.getParent());
        boolean existed = Files.exists(filePath);
        entries.add(null);
        if (existed) {
            for(String line : Files.readAllLines(filePath, StandardCharsets.UTF_8)) {
                if(line.isBlank())
                    continue;
                LogEntry entry = LogEntry.deserialize(line);
                long expectedIndex = entries.size();
                if(entry.index() != expectedIndex)
                    throw new IllegalStateException("Log file is corrupted. Expected index " + expectedIndex + " but got " + entry.index());
                entries.add(entry);
            }
        } 
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    public synchronized void append(LogEntry entry) throws IOException {
        long expectedIndex = lastIndex() + 1;
        if (entry.index() != expectedIndex)
            throw new IllegalArgumentException("Out-of-order append: expected index " + expectedIndex + " but got " + entry.index());
        String line = entry.serialize() + "\n";
        raf.write(line.getBytes(StandardCharsets.UTF_8));
        channel.force(true);
        entries.add(entry);
    }

    public synchronized void truncateFrom(long fromIndex) throws IOException {
        if (fromIndex > lastIndex()) return;
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
        }
        channel.close();
        raf.close();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            if (e != null) sb.append(e.serialize()).append('\n');
        }
        Files.writeString(filePath, sb.toString(), StandardCharsets.UTF_8);
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    public synchronized Optional<LogEntry> get(long index) {
        if (index <= 0 || index >= entries.size()) 
            return Optional.empty();
        return Optional.ofNullable(entries.get((int) index));
    }

    public synchronized long lastIndex() {
        return entries.size() - 1;
    }

    public synchronized long lastTerm() {
        return get(lastIndex()).map(LogEntry::term).orElse(0L);
    }

    public synchronized boolean isCandidateLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastTerm();
        if (candidateLastTerm != myLastTerm)
            return candidateLastTerm > myLastTerm;
        return candidateLastIndex >= lastIndex();
    }

    public synchronized void close() throws IOException {
        if (channel != null) 
            channel.close();
        if (raf != null) 
            raf.close();
    }
}