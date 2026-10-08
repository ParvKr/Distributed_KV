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

/**
 * The Raft log, persisted one entry per line. Entries up to {@code baseIndex} have been folded into a
 * snapshot and are no longer stored; the log holds entries {@code baseIndex + 1 .. lastIndex()}.
 */
public final class PersistentLog implements AutoCloseable {
    private final Path filePath;
    private final List<LogEntry> entries = new ArrayList<>();
    private long baseIndex = 0;
    private long baseTerm = 0;
    private RandomAccessFile raf;
    private FileChannel channel;

    public PersistentLog(String dataDir) {
        this.filePath = Path.of(dataDir, "raft.log");
    }

    public synchronized void init() throws IOException {
        init(0, 0);
    }

    /**
     * @param baseIndex index of the last entry covered by the node's snapshot (0 if none)
     * @param baseTerm  term of that entry
     */
    public synchronized void init(long baseIndex, long baseTerm) throws IOException {
        if(raf != null || channel != null)
            throw new IllegalStateException("PersistentLog is already initialized");
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        Files.createDirectories(filePath.getParent());
        boolean needsRewrite = false;
        if (Files.exists(filePath)) {
            String content = Files.readString(filePath, StandardCharsets.UTF_8);
            // Every append writes "<entry>\n" in one go, so a final segment without a newline is a
            // write that was cut short by a crash. It was never acknowledged; drop it.
            String[] lines = content.split("\n", -1);
            needsRewrite = !lines[lines.length - 1].isEmpty();
            for (int i = 0; i < lines.length - 1; i++) {
                String line = lines[i];
                if (line.isBlank())
                    continue;
                LogEntry entry = LogEntry.deserialize(line);
                if (entry.index() <= baseIndex) {
                    // A crash between writing the snapshot and compacting the log leaves entries the
                    // snapshot already covers.
                    needsRewrite = true;
                    continue;
                }
                long expectedIndex = lastIndex() + 1;
                if (entry.index() != expectedIndex)
                    throw new IllegalStateException("Log file is corrupted. Expected index " + expectedIndex + " but got " + entry.index());
                entries.add(entry);
            }
        }
        if (needsRewrite)
            rewriteFile();
        openForAppend();
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
        if (fromIndex <= baseIndex)
            throw new IllegalArgumentException("Cannot truncate from index " + fromIndex
                + "; entries up to " + baseIndex + " are covered by the snapshot (and indexes start at 1)");
        if (fromIndex > lastIndex()) return;
        while (lastIndex() >= fromIndex) {
            entries.remove(entries.size() - 1);
        }
        rewriteAndReopen();
    }

    /**
     * Drops every entry up to and including {@code index} because a snapshot now covers them, and makes
     * {@code (index, term)} the new base. Entries after {@code index} are kept if the log really contains
     * the snapshot's last entry; otherwise (the log is shorter, or disagrees with the snapshot) the whole
     * log is discarded.
     */
    public synchronized void compactThrough(long index, long term) throws IOException {
        if (index <= baseIndex)
            return;
        Optional<LogEntry> boundary = get(index);
        if (boundary.isPresent() && boundary.get().term() == term) {
            entries.subList(0, (int) (index - baseIndex)).clear();
        } else {
            entries.clear();
        }
        baseIndex = index;
        baseTerm = term;
        rewriteAndReopen();
    }

    private void rewriteAndReopen() throws IOException {
        channel.close();
        raf.close();
        rewriteFile();
        openForAppend();
    }

    private void openForAppend() throws IOException {
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    private void rewriteFile() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : entries) {
            sb.append(e.serialize()).append('\n');
        }
        FileUtil.writeAtomically(filePath, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    public synchronized Optional<LogEntry> get(long index) {
        if (index <= baseIndex || index > lastIndex())
            return Optional.empty();
        return Optional.of(entries.get((int) (index - baseIndex - 1)));
    }

    /**
     * The term of the entry at {@code index}, including the snapshot boundary entry (and index 0, whose
     * term is 0). Empty for entries that were compacted away or do not exist yet.
     */
    public synchronized Optional<Long> termAt(long index) {
        if (index == baseIndex)
            return Optional.of(baseTerm);
        return get(index).map(LogEntry::term);
    }

    public synchronized long snapshotIndex() {
        return baseIndex;
    }

    public synchronized long snapshotTerm() {
        return baseTerm;
    }

    public synchronized long lastIndex() {
        return baseIndex + entries.size();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty() ? baseTerm : entries.get(entries.size() - 1).term();
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
