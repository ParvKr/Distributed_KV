package io.github.parvgurung.log;

import io.github.parvgurung.util.FileUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
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

    // Group commit: entries can be appended without an fsync and made durable later, one fsync covering
    // everything appended so far. durableIndex is the highest index known to be on disk.
    private long durableIndex = 0;
    private boolean syncing = false;
    private long syncCount = 0;

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
            byte[] bytes = Files.readAllBytes(filePath);
            // Every append writes "<entry>\n" in one go, so bytes after the last newline are a write that
            // was cut short by a crash (possibly in the middle of a multi-byte character, which is why we
            // split on raw bytes before decoding). It was never acknowledged; drop it.
            int complete = bytes.length;
            while (complete > 0 && bytes[complete - 1] != '\n') complete--;
            needsRewrite = complete != bytes.length;
            String content = new String(bytes, 0, complete, StandardCharsets.UTF_8);
            String[] lines = content.split("\n", -1);
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
            rewriteFile(entries);
        openForAppend();
        durableIndex = lastIndex();   // whatever we just read is on disk
    }

    public synchronized void append(LogEntry entry) throws IOException {
        appendAll(List.of(entry));
    }

    /** Appends several entries with a single fsync. All of them are written, or (on failure) none are kept. */
    public synchronized void appendAll(List<LogEntry> batch) throws IOException {
        if (batch.isEmpty())
            return;
        long expectedIndex = lastIndex() + 1;
        StringBuilder text = new StringBuilder();
        for (LogEntry entry : batch) {
            if (entry.index() != expectedIndex)
                throw new IllegalArgumentException("Out-of-order append: expected index " + expectedIndex + " but got " + entry.index());
            text.append(entry.serialize()).append('\n');
            expectedIndex++;
        }
        writeToFile(text.toString(), true);
        entries.addAll(batch);
        durableIndex = lastIndex();   // the fsync above also covered anything appended earlier without one
    }

    /**
     * Appends one entry to the file and memory but does not fsync. It is not durable until
     * {@link #syncUpTo} has returned for its index; until then it must not be counted as stored.
     * Cheap enough to call while holding a lock, which is the point: the slow fsync happens elsewhere.
     */
    public synchronized void appendWithoutSync(LogEntry entry) throws IOException {
        long expectedIndex = lastIndex() + 1;
        if (entry.index() != expectedIndex)
            throw new IllegalArgumentException("Out-of-order append: expected index " + expectedIndex + " but got " + entry.index());
        writeToFile(entry.serialize() + "\n", false);
        entries.add(entry);
    }

    private void writeToFile(String text, boolean sync) throws IOException {
        ensureOpen();
        long startPos = raf.length();
        try {
            raf.write(text.getBytes(StandardCharsets.UTF_8));
            if (sync) {
                channel.force(true);
                syncCount++;
            }
        } catch (IOException e) {
            // Don't leave partial lines behind for the next append to be glued onto.
            try {
                raf.setLength(startPos);
                raf.seek(startPos);
            } catch (IOException ignored) {
            }
            throw e;
        }
    }

    /**
     * Blocks until the entry at {@code index} (and everything before it) is on disk. Callers that arrive while
     * an fsync is already running wait for it, and a single fsync makes every entry appended before it started
     * durable, so concurrent writers share fsyncs instead of each paying for their own.
     */
    public void syncUpTo(long index) throws IOException {
        final FileChannel toSync;
        final long covers;
        synchronized (this) {
            while (true) {
                if (durableIndex >= index || index > lastIndex())
                    return;   // already durable, or the entry no longer exists (the log was rewritten)
                if (!syncing)
                    break;
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for the log to be synced", e);
                }
            }
            syncing = true;
            ensureOpen();
            toSync = channel;
            covers = lastIndex();   // everything appended up to now is written, so this fsync will cover it
        }
        boolean succeeded = false;
        try {
            toSync.force(true);
            succeeded = true;
        } catch (ClosedChannelException e) {
            // The file was replaced by a rewrite (compaction or truncation) or the log was closed. A rewrite
            // fsyncs everything it keeps and updates durableIndex itself, so there is nothing left to do.
        } finally {
            synchronized (this) {
                syncing = false;
                if (succeeded) {
                    syncCount++;
                    durableIndex = Math.max(durableIndex, Math.min(covers, lastIndex()));
                }
                notifyAll();
            }
        }
    }

    /** The highest log index known to be on disk. */
    public synchronized long durableIndex() {
        return durableIndex;
    }

    /** How many fsyncs this log has performed (for tests and benchmarks). */
    public synchronized long syncCount() {
        return syncCount;
    }

    public synchronized void truncateFrom(long fromIndex) throws IOException {
        if (fromIndex <= baseIndex)
            throw new IllegalArgumentException("Cannot truncate from index " + fromIndex
                + "; entries up to " + baseIndex + " are covered by the snapshot (and indexes start at 1)");
        if (fromIndex > lastIndex()) return;
        List<LogEntry> kept = new ArrayList<>(entries.subList(0, (int) (fromIndex - baseIndex - 1)));
        replaceContents(kept, baseIndex, baseTerm);
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
        List<LogEntry> kept = boundary.isPresent() && boundary.get().term() == term
            ? new ArrayList<>(entries.subList((int) (index - baseIndex), entries.size()))
            : new ArrayList<>();
        replaceContents(kept, index, term);
    }

    /**
     * Makes the file hold exactly {@code newEntries}, and only then updates memory. If writing the file
     * fails, nothing has changed and memory still matches the old (intact) file.
     */
    private void replaceContents(List<LogEntry> newEntries, long newBaseIndex, long newBaseTerm) throws IOException {
        rewriteFile(newEntries);
        entries.clear();
        entries.addAll(newEntries);
        baseIndex = newBaseIndex;
        baseTerm = newBaseTerm;
        durableIndex = lastIndex();   // the atomic rewrite fsynced everything it kept
        closeQuietly();
        openForAppend();
        notifyAll();                  // wake any syncUpTo waiting on entries that no longer exist
    }

    private void closeQuietly() {
        try {
            if (channel != null) channel.close();
            if (raf != null) raf.close();
        } catch (IOException ignored) {
        }
        channel = null;
        raf = null;
    }

    private void ensureOpen() throws IOException {
        if (raf == null || channel == null || !channel.isOpen())
            openForAppend();
    }

    private void openForAppend() throws IOException {
        raf = new RandomAccessFile(filePath.toFile(), "rw");
        raf.seek(raf.length());
        channel = raf.getChannel();
    }

    private void rewriteFile(List<LogEntry> toWrite) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : toWrite) {
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
