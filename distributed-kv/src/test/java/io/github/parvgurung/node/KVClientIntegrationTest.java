package io.github.parvgurung.node;

import io.github.parvgurung.client.KVClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class KVClientIntegrationTest {
    @TempDir
    Path tempDir;

    private TestCluster cluster;
    private RaftNode leader;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(3, tempDir, 10);
        cluster.startAll();
        cluster.awaitCondition("settled", 15_000, () -> cluster.runningNodes().stream().allMatch(n -> n.commitIndex() >= 1));
        leader = cluster.awaitLeader(10_000);
    }

    @AfterEach
    void stop() throws Exception {
        cluster.close();
    }

    private List<String> addresses(int... ids) {
        List<String> list = new ArrayList<>();
        for (int id : ids) list.add("localhost:" + cluster.port(id));
        return list;
    }

    private int followerId() {
        return cluster.runningNodes().stream().filter(n -> n != leader).findFirst().orElseThrow().nodeId();
    }

    @Test
    void setGetDeleteRoundTrip() throws Exception {
        KVClient client = new KVClient(addresses(1, 2, 3));
        assertNull(client.get("missing"));
        client.set("k", "v");
        assertEquals("v", client.get("k"));
        client.set("k", "v2");
        assertEquals("v2", client.get("k"));
        client.delete("k");
        assertNull(client.get("k"));
        client.delete("never-existed"); // deleting nothing is fine
    }

    @Test
    void worksWhenOnlyAFollowerIsListed() throws Exception {
        KVClient client = new KVClient(addresses(followerId()));
        client.set("via-follower", "ok");   // the follower redirects to the leader
        assertEquals("ok", client.get("via-follower"));
    }

    @Test
    void skipsDeadAddressesInTheList() throws Exception {
        KVClient client = new KVClient(List.of("localhost:1", "localhost:2", "localhost:" + cluster.port(leader.nodeId())));
        client.set("k", "v");
        assertEquals("v", client.get("k"));
    }

    @Test
    void keysAndValuesWithAwkwardCharactersRoundTripThroughTheClientLibrary() throws Exception {
        KVClient client = new KVClient(addresses(1, 2, 3));
        String[] keys = {"a&b=c", "plus+sign", "percent%41", "space key", "slash/and?question#hash", "日本語", "emoji😀", "new\nline", "semi;colon", "a=b&c=d"};
        for (String key : keys) {
            client.set(key, "value of " + key);
        }
        for (String key : keys) {
            assertEquals("value of " + key, client.get(key), "key " + key.replace("\n", "\\n"));
        }
    }

    @Test
    void oneClientSharedByManyThreadsKeepsEveryWrite() throws Exception {
        KVClient client = new KVClient(addresses(1, 2, 3));
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<?>> done = new ArrayList<>();
        for (int t = 0; t < 6; t++) {
            final int id = t;
            done.add(pool.submit(() -> {
                for (int i = 0; i < 8; i++) {
                    try {
                        client.set("t" + id + "-" + i, "v" + i);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }
        for (Future<?> f : done) f.get(60, TimeUnit.SECONDS);
        pool.shutdownNow();
        for (int t = 0; t < 6; t++) for (int i = 0; i < 8; i++) assertEquals("v" + i, client.get("t" + t + "-" + i));
    }

    @Test
    void anUnreachableClusterFailsWithAnExplanationRatherThanHanging() {
        KVClient client = new KVClient(List.of("localhost:1", "localhost:2"));
        long start = System.currentTimeMillis();
        IOException e = assertThrows(IOException.class, () -> client.set("k", "v"));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 20_000, "gave up after " + elapsed + "ms");
        assertTrue(e.getMessage().contains("connection error") || e.getMessage().contains("within"), e.getMessage());
    }

    @Test
    void invalidArgumentsAreRejectedBeforeAnythingIsSent() {
        KVClient client = new KVClient(addresses(1, 2, 3));
        assertThrows(IllegalArgumentException.class, () -> client.set("", "v"));
        assertThrows(IllegalArgumentException.class, () -> client.set("k", null));
        assertThrows(IllegalArgumentException.class, () -> client.get(null));
        assertThrows(IllegalArgumentException.class, () -> client.delete(""));
        assertThrows(IllegalArgumentException.class, () -> new KVClient(List.of()));
    }
}
