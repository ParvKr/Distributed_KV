package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class GroupCommitTest {
    @TempDir
    Path tempDir;

    private static LogEntry entry(long index) {
        return new LogEntry(index, 1, Command.set("k" + index, "v" + index));
    }

    @Test
    void anEntryAppendedWithoutSyncIsNotDurableUntilSynced() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            log.appendWithoutSync(entry(1));
            assertEquals(1, log.lastIndex());
            assertEquals(0, log.durableIndex(), "not durable yet");
            log.syncUpTo(1);
            assertEquals(1, log.durableIndex());
        }
    }

    @Test
    void syncedEntriesSurviveARestart() throws IOException {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            for (int i = 1; i <= 20; i++) log.appendWithoutSync(entry(i));
            log.syncUpTo(20);
        }
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            assertEquals(20, log.lastIndex());
            assertEquals(20, log.durableIndex(), "everything read from disk counts as durable");
        }
    }

    @Test
    void manyWritersWaitingTogetherShareASingleFsync() throws Exception {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            int writers = 64;
            for (int i = 1; i <= writers; i++) log.appendWithoutSync(entry(i));   // all written, none synced
            long before = log.syncCount();

            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> done = new ArrayList<>();
            for (int i = 1; i <= writers; i++) {
                final long index = i;
                done.add(pool.submit(() -> {
                    go.await();
                    log.syncUpTo(index);
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : done) f.get(10, TimeUnit.SECONDS);
            pool.shutdownNow();

            assertEquals(writers, log.durableIndex());
            long fsyncs = log.syncCount() - before;
            assertTrue(fsyncs >= 1 && fsyncs <= 2, "64 waiting writers should need at most 2 fsyncs, but used " + fsyncs);
        }
    }

    @Test
    void writersThatArriveWhileASyncIsRunningAreCoveredByTheNextOne() throws Exception {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            ExecutorService pool = Executors.newFixedThreadPool(16);
            List<Future<?>> done = new ArrayList<>();
            for (int w = 0; w < 16; w++) {
                done.add(pool.submit(() -> {
                    for (int i = 0; i < 25; i++) {
                        long index;
                        synchronized (log) {            // like the node lock: index assignment + append
                            index = log.lastIndex() + 1;
                            log.appendWithoutSync(entry(index));
                        }
                        log.syncUpTo(index);
                        assertTrue(log.durableIndex() >= index, "syncUpTo returned before the entry was durable");
                    }
                    return null;
                }));
            }
            for (Future<?> f : done) f.get(60, TimeUnit.SECONDS);
            pool.shutdownNow();
            assertEquals(400, log.lastIndex());
            assertEquals(400, log.durableIndex());
            assertTrue(log.syncCount() < 400, "400 concurrent writes should share fsyncs, but used " + log.syncCount());
        }
    }

    @Test
    void syncingAnEntryThatWasTruncatedAwayReturnsInsteadOfHanging() throws Exception {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            for (int i = 1; i <= 5; i++) log.appendWithoutSync(entry(i));
            log.truncateFrom(3);                    // entries 3-5 are gone
            log.syncUpTo(5);                        // must not block forever
            assertEquals(2, log.lastIndex());
            assertEquals(2, log.durableIndex(), "the rewrite is durable by itself");
        }
    }

    @Test
    void compactionAndTruncationKeepTheDurableIndexConsistent() throws Exception {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            for (int i = 1; i <= 6; i++) log.appendWithoutSync(entry(i));
            log.compactThrough(4, 1);
            assertEquals(6, log.durableIndex());
            log.appendWithoutSync(entry(7));
            assertEquals(6, log.durableIndex());
            log.syncUpTo(7);
            assertEquals(7, log.durableIndex());
        }
    }

    @Test
    void aSynchronousAppendAlsoMakesEarlierUnsyncedEntriesDurable() throws Exception {
        try (PersistentLog log = new PersistentLog(tempDir.toString())) {
            log.init();
            log.appendWithoutSync(entry(1));
            log.append(entry(2));     // the normal fsync'd append, used by followers
            assertEquals(2, log.durableIndex());
        }
    }
}
