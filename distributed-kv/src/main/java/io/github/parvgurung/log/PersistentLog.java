package io.github.parvgurung.log;

import io.github.parvgurung.util.FileUtil;

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
        boolean tornTail = false;
        if (existed) {
            String content = Files.readString(filePath, StandardCharsets.UTF_8);
            // Every append writes "<entry>\n" in one go, so a final segment without a newline is a
            // write that was cut short by a crash. It was never acknowledged; drop it.
            String[] lines = content.split("\n", -1);
            tornTail = !lines[lines.length - 1].isEmpty();
            for (int i = 0; i < lines.length - 1; i++) {
                String line = lines[i];
                if (line.isBlank())
                    continue;
                LogEntry entry = LogEntry.deserialize(line);
                long expectedIndex = entries.size();
                if (entry.index() != expectedIndex)
                    throw new IllegalStateException("Log file is corrupted. Expected index " + expectedIndex + " but got " + entry.index());
                entries.add(entry);
            }
        }
        if (tornTail)
            rewriteFile();
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    public synchronized void append(LogEntry entry) throws IOException {
        long expectedIndex = lastIndex() + 1;
        if (entry.index() != expectedIndex)
            throw new IllegalArgumentException("Out-of-order append: expected index " + expectedIndex + " but got " + entry.index());
        String line = entry.serialize() + "\n";
        long startPos = raf.length();
        try {
            raf.write(line.getBytes(StandardCharsets.UTF_8));
            channel.force(true);
        } catch (IOException e) {
            // Don't leave a partial line behind for the next append to be glued onto.
            try {
                raf.setLength(startPos);
                raf.seek(startPos);
            } catch (IOException ignored) {
            }
            throw e;
        }
        entries.add(entry);
    }

    public synchronized void truncateFrom(long fromIndex) throws IOException {
        if (fromIndex < 1)
            throw new IllegalArgumentException("Cannot truncate from index " + fromIndex + "; indexes start at 1");
        if (fromIndex > lastIndex()) return;
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
        }
        channel.close();
        raf.close();
        rewriteFile();
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    private void rewriteFile() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < entries.size(); i++) {
            sb.append(entries.get(i).serialize()).append('\n');
        }
        FileUtil.writeAtomically(filePath, sb.toString().getBytes(StandardCharsets.UTF_8));
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