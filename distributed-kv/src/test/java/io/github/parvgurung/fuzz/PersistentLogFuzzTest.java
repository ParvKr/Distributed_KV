package io.github.parvgurung.fuzz;

import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.log.PersistentLog;
import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class PersistentLogFuzzTest {
    @TempDir
    Path tempDir;

    /** The simplest possible correct log: a list plus a base. */
    private static final class Model {
        long baseIndex = 0, baseTerm = 0;
        final List<LogEntry> entries = new ArrayList<>();

        long lastIndex() { return baseIndex + entries.size(); }
        long lastTerm() { return entries.isEmpty() ? baseTerm : entries.get(entries.size() - 1).term(); }
        Optional<LogEntry> get(long i) {
            return i <= baseIndex || i > lastIndex() ? Optional.empty() : Optional.of(entries.get((int) (i - baseIndex - 1)));
        }
    }

    // Escape control characters so failure messages stay on one readable line.
    private static String show(Optional<LogEntry> e) {
        StringBuilder sb = new StringBuilder();
        for (char c : e.toString().toCharArray()) {
            if (c < 0x20 || c > 0x7e) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c);
        }
        return sb.toString();
    }

    private static void assertSame(Model m, PersistentLog log, String ctx) {
        assertEquals(m.baseIndex, log.snapshotIndex(), ctx);
        assertEquals(m.baseTerm, log.snapshotTerm(), ctx);
        assertEquals(m.lastIndex(), log.lastIndex(), ctx);
        assertEquals(m.lastTerm(), log.lastTerm(), ctx);
        for (long i = 0; i <= m.lastIndex() + 2; i++) {
            assertEquals(show(m.get(i)), show(log.get(i)), ctx + " get(" + i + ")");
        }
    }

    @Test
    void randomOperationsAndRestartsMatchTheModel() throws IOException {
        for (long seed = 1; seed <= 10; seed++) {
            Path dir = tempDir.resolve("seed" + seed);
            Random r = new Random(seed);
            Model m = new Model();
            PersistentLog log = new PersistentLog(dir.toString());
            log.init();
            String ctx = "seed " + seed;
            for (int step = 0; step < 100; step++) {
                ctx = "seed " + seed + " step " + step;
                switch (r.nextInt(10)) {
                    case 0, 1, 2, 3, 4 -> {   // append
                        LogEntry e = new LogEntry(m.lastIndex() + 1, 1 + r.nextInt(5),
                            Command.set(Gen.nonEmptyString(r, 6), Gen.string(r, 10)));
                        log.append(e);
                        m.entries.add(e);
                    }
                    case 5 -> {               // truncate a suffix
                        if (m.entries.isEmpty()) break;
                        long from = m.baseIndex + 1 + r.nextInt(m.entries.size());
                        log.truncateFrom(from);
                        while (m.lastIndex() >= from) m.entries.remove(m.entries.size() - 1);
                    }
                    case 6 -> {               // compact up to an entry that exists
                        if (m.entries.isEmpty()) break;
                        long upTo = m.baseIndex + 1 + r.nextInt(m.entries.size());
                        long term = m.get(upTo).orElseThrow().term();
                        log.compactThrough(upTo, term);
                        m.entries.subList(0, (int) (upTo - m.baseIndex)).clear();
                        m.baseIndex = upTo;
                        m.baseTerm = term;
                    }
                    case 7 -> {               // install a snapshot beyond the whole log (or one that disagrees)
                        long upTo = m.lastIndex() + 1 + r.nextInt(5);
                        long term = 6 + r.nextInt(3);
                        log.compactThrough(upTo, term);
                        m.entries.clear();
                        m.baseIndex = upTo;
                        m.baseTerm = term;
                    }
                    default -> {              // restart from disk
                        log.close();
                        log = new PersistentLog(dir.toString());
                        log.init(m.baseIndex, m.baseTerm);
                    }
                }
                assertSame(m, log, ctx);
            }
            log.close();
            PersistentLog reopened = new PersistentLog(dir.toString());
            reopened.init(m.baseIndex, m.baseTerm);
            assertSame(m, reopened, ctx + " after final restart");
            reopened.close();
        }
    }

    @Test
    void aCrashCutAtAnyByteOffsetRecoversExactlyTheCompleteEntries() throws IOException {
        Random r = new Random(11);
        Path source = tempDir.resolve("source");
        List<LogEntry> entries = new ArrayList<>();
        try (PersistentLog log = new PersistentLog(source.toString())) {
            log.init();
            for (int i = 1; i <= 6; i++) {
                // non-ASCII values, so some cuts land in the middle of a multi-byte character
                LogEntry e = new LogEntry(i, 1 + i / 3, Command.set("key" + i, "\u65e5\u672c" + Gen.string(r, 4) + "\uD83D\uDE00", "client", i));
                log.append(e);
                entries.add(e);
            }
        }
        byte[] bytes = Files.readAllBytes(source.resolve("raft.log"));
        for (int cut = 0; cut <= bytes.length; cut++) {
            Path dir = tempDir.resolve("cut" + cut);
            Files.createDirectories(dir);
            Files.write(dir.resolve("raft.log"), java.util.Arrays.copyOf(bytes, cut));

            int completeLines = 0;
            for (int i = 0; i < cut; i++) if (bytes[i] == '\n') completeLines++;

            try (PersistentLog log = new PersistentLog(dir.toString())) {
                log.init();
                assertEquals(completeLines, log.lastIndex(), "cut at byte " + cut);
                for (int i = 1; i <= completeLines; i++) assertEquals(entries.get(i - 1), log.get(i).orElseThrow());
                if (cut % 9 == 0) {
                    // it must keep working: the next append lands cleanly after the recovered prefix
                    log.append(new LogEntry(completeLines + 1, 9, Command.set("after", "crash")));
                }
            }
            if (cut % 9 == 0) {
                try (PersistentLog log = new PersistentLog(dir.toString())) {
                    log.init();
                    assertEquals(completeLines + 1, log.lastIndex(), "after re-open, cut at byte " + cut);
                }
            }
        }
    }
}
