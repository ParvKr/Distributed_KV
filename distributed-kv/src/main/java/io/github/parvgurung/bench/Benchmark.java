package io.github.parvgurung.bench;

import io.github.parvgurung.client.KVClient;
import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.node.RaftNode;
import io.github.parvgurung.node.NodeRole;
import io.github.parvgurung.rpc.ClientResponse;
import io.github.parvgurung.util.Json;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Throughput, latency and failover measurements.
 *
 * <pre>
 *   java -cp raftkv.jar io.github.parvgurung.bench.Benchmark local  [--nodes=3] [--clients=16] [--seconds=10] [--valueBytes=100]
 *   java -cp raftkv.jar io.github.parvgurung.bench.Benchmark remote --nodes=host:port,host:port,host:port [--clients=16] [--seconds=10]
 * </pre>
 *
 * "local" starts a cluster inside this JVM (temp directories, real HTTP and real fsyncs) and also measures
 * failover and follower catch-up, which need to kill nodes. "remote" runs only the read/write workloads
 * against a cluster you started yourself (for example with docker compose).
 */
public final class Benchmark {
    private final int clients;
    private final int seconds;
    private final int valueBytes;
    private final int failovers;
    private final int catchUpEntries;
    private final int snapshotThreshold;

    private Benchmark(Map<String, String> options) {
        this.clients = intOption(options, "clients", 16);
        this.seconds = intOption(options, "seconds", 10);
        this.valueBytes = intOption(options, "valueBytes", 100);
        this.failovers = intOption(options, "failovers", 10);
        this.catchUpEntries = intOption(options, "catchUpEntries", 5000);
        this.snapshotThreshold = intOption(options, "snapshotThreshold", NodeConfig.DEFAULT_SNAPSHOT_THRESHOLD);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !(args[0].equals("local") || args[0].equals("remote"))) {
            System.err.println("Usage: Benchmark local|remote [--nodes=...] [--clients=16] [--seconds=10] [--valueBytes=100]"
                + " [--failovers=10] [--catchUpEntries=5000] [--snapshotThreshold=1000]");
            System.exit(1);
        }
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--") || !args[i].contains("="))
                throw new IllegalArgumentException("Malformed argument: " + args[i]);
            String[] kv = args[i].substring(2).split("=", 2);
            options.put(kv[0], kv[1]);
        }
        Benchmark benchmark = new Benchmark(options);
        if (args[0].equals("local")) {
            benchmark.runLocal(intOption(options, "nodes", 3));
        } else {
            String nodes = options.get("nodes");
            if (nodes == null) throw new IllegalArgumentException("remote mode needs --nodes=host:port,...");
            benchmark.runRemote(Arrays.asList(nodes.split(",")));
        }
    }

    private static int intOption(Map<String, String> options, String name, int fallback) {
        String v = options.get(name);
        return v == null ? fallback : Integer.parseInt(v);
    }

    // ------------------------------------------------------------------ modes

    private void runRemote(List<String> addresses) throws Exception {
        printEnvironment("remote cluster " + addresses);
        List<Result> results = new ArrayList<>();
        results.add(writeWorkload(addresses));
        results.add(readWorkload(addresses));
        printResults(results);
    }

    private void runLocal(int nodeCount) throws Exception {
        Path root = Files.createTempDirectory("raftkv-bench");
        LocalCluster cluster = new LocalCluster(nodeCount, root, snapshotThreshold);
        try {
            cluster.startAll();
            cluster.awaitLeader(15_000);
            printEnvironment(nodeCount + "-node cluster in one JVM, real HTTP, fsync on every append, snapshot threshold "
                + (snapshotThreshold == 0 ? "off" : snapshotThreshold));

            List<String> addresses = cluster.addresses();
            List<Result> results = new ArrayList<>();
            System.out.println("Warming up...");
            runWorkload("warm-up", addresses, 2, true);
            Map<Integer, Long> syncsBefore = cluster.syncCounts();
            Result writes = writeWorkload(addresses);
            Map<Integer, Long> syncsAfter = cluster.syncCounts();
            results.add(writes);
            results.add(readWorkload(addresses));
            printResults(results);
            System.out.println();
            System.out.print("fsyncs per write during the write phase:");
            for (Map.Entry<Integer, Long> e : new TreeMap<>(syncsAfter).entrySet()) {
                double perWrite = (e.getValue() - syncsBefore.getOrDefault(e.getKey(), 0L)) / (double) writes.operations();
                System.out.printf(" node %d = %.2f", e.getKey(), perWrite);
            }
            System.out.println();

            if (nodeCount >= 3) {
                System.out.println();
                failoverBenchmark(cluster);
                System.out.println();
                catchUpBenchmark(cluster);
            }
        } finally {
            cluster.close();
            deleteRecursively(root);
        }
    }

    // ------------------------------------------------------------------ read/write workloads

    private record Result(String name, long operations, long errors, double seconds, long[] latenciesNanos) {
        double throughput() { return operations / seconds; }
        double percentileMs(double p) {
            if (latenciesNanos.length == 0) return Double.NaN;
            int index = (int) Math.min(latenciesNanos.length - 1, Math.ceil(p / 100.0 * latenciesNanos.length) - 1);
            return latenciesNanos[Math.max(0, index)] / 1_000_000.0;
        }
    }

    private Result writeWorkload(List<String> addresses) throws Exception {
        System.out.println("Measuring writes (" + clients + " clients, " + seconds + " s, " + valueBytes + "-byte values)...");
        return runWorkload("writes", addresses, seconds, true);
    }

    private Result readWorkload(List<String> addresses) throws Exception {
        // Make sure there is something to read.
        KVClient seeder = new KVClient(addresses);
        int keys = 200;
        for (int i = 0; i < keys; i++) seeder.set("read-key-" + i, "x".repeat(valueBytes));
        System.out.println("Measuring reads (" + clients + " clients, " + seconds + " s)...");
        return runWorkload("reads", addresses, seconds, false);
    }

    private Result runWorkload(String name, List<String> addresses, int durationSeconds, boolean write) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<long[]>> futures = new ArrayList<>();
        AtomicLong errors = new AtomicLong();
        String value = "x".repeat(valueBytes);
        long runId = System.nanoTime();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
        for (int c = 0; c < clients; c++) {
            final int id = c;
            futures.add(pool.submit(() -> {
                KVClient client = new KVClient(addresses);   // one client (and session) per thread
                Random random = new Random(id);
                long[] samples = new long[1 << 16];
                int count = 0;
                long n = 0;
                while (System.nanoTime() < deadline) {
                    long start = System.nanoTime();
                    try {
                        if (write) client.set("w-" + runId + "-" + id + "-" + n++, value);
                        else client.get("read-key-" + random.nextInt(200));
                    } catch (IOException e) {
                        errors.incrementAndGet();
                        continue;
                    }
                    long took = System.nanoTime() - start;
                    if (count == samples.length) samples = Arrays.copyOf(samples, count * 2);
                    samples[count++] = took;
                }
                return Arrays.copyOf(samples, count);
            }));
        }
        long begin = System.nanoTime();
        List<long[]> parts = new ArrayList<>();
        for (Future<long[]> f : futures) parts.add(f.get());
        double elapsed = (System.nanoTime() - begin) / 1e9;
        pool.shutdownNow();
        int total = parts.stream().mapToInt(a -> a.length).sum();
        long[] all = new long[total];
        int pos = 0;
        for (long[] part : parts) { System.arraycopy(part, 0, all, pos, part.length); pos += part.length; }
        Arrays.sort(all);
        return new Result(name, total, errors.get(), Math.max(elapsed, durationSeconds), all);
    }

    // ------------------------------------------------------------------ failover

    private void failoverBenchmark(LocalCluster cluster) throws Exception {
        System.out.println("Measuring failover (" + failovers + " leader kills)...");
        List<Double> electionMs = new ArrayList<>();
        List<Double> writeMs = new ArrayList<>();
        for (int round = 0; round < failovers; round++) {
            RaftNode leader = cluster.awaitLeader(15_000);
            int killed = leader.nodeId();
            long t0 = System.nanoTime();
            cluster.crash(killed);

            long electedAt = -1, writableAt = -1;
            long deadline = t0 + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline && (electedAt < 0 || writableAt < 0)) {
                if (electedAt < 0 && cluster.hasLeader()) electedAt = System.nanoTime();
                if (writableAt < 0 && cluster.tryWrite("failover-" + round)) writableAt = System.nanoTime();
                Thread.sleep(2);
            }
            if (electedAt > 0) electionMs.add((electedAt - t0) / 1e6);
            if (writableAt > 0) writeMs.add((writableAt - t0) / 1e6);

            cluster.start(killed);
            cluster.awaitCaughtUp(30_000);
        }
        System.out.println("| Failover (leader killed) | median | p95 | max |");
        System.out.println("|---|---|---|---|");
        printRow("New leader elected", electionMs);
        printRow("First write accepted again", writeMs);
    }

    private static void printRow(String label, List<Double> values) {
        if (values.isEmpty()) {
            System.out.printf("| %s | n/a | n/a | n/a |%n", label);
            return;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        double median = sorted.get(sorted.size() / 2);
        double p95 = sorted.get((int) Math.min(sorted.size() - 1, Math.ceil(0.95 * sorted.size()) - 1));
        System.out.printf("| %s | %.0f ms | %.0f ms | %.0f ms |%n", label, median, p95, sorted.get(sorted.size() - 1));
    }

    // ------------------------------------------------------------------ follower catch-up

    private void catchUpBenchmark(LocalCluster cluster) throws Exception {
        System.out.println("Measuring follower catch-up (" + catchUpEntries + " missed writes)...");
        RaftNode leader = cluster.awaitLeader(15_000);
        int sleeper = cluster.anyFollower(leader.nodeId());
        cluster.crash(sleeper);

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<?>> writes = new ArrayList<>();
        String value = "x".repeat(valueBytes);
        for (int c = 0; c < clients; c++) {
            final int id = c;
            writes.add(pool.submit(() -> {
                KVClient own = new KVClient(cluster.addresses());
                for (int i = id; i < catchUpEntries; i += clients) {
                    try { own.set("catchup-" + i, value); } catch (IOException e) { throw new RuntimeException(e); }
                }
                return null;
            }));
        }
        for (Future<?> f : writes) f.get();
        pool.shutdownNow();

        long target = cluster.awaitLeader(15_000).commitIndex();
        long t0 = System.nanoTime();
        cluster.start(sleeper);
        cluster.awaitCommit(sleeper, target, 120_000);
        double ms = (System.nanoTime() - t0) / 1e6;
        System.out.printf("| Follower catch-up | %,d missed writes | %.0f ms |%n", catchUpEntries, ms);
        System.out.println("(restart to fully caught up, "
            + (snapshotThreshold == 0 ? "replaying the log" : "via snapshot plus log tail when the log was compacted past it") + ")");
    }

    // ------------------------------------------------------------------ output

    private void printEnvironment(String target) {
        System.out.println("## Environment");
        System.out.println("- Target: " + target);
        System.out.println("- Machine: " + System.getProperty("os.name") + " " + System.getProperty("os.arch")
            + ", " + Runtime.getRuntime().availableProcessors() + " cores, JVM " + System.getProperty("java.version"));
        System.out.println("- Workload: " + clients + " client threads, " + seconds + " s per phase, " + valueBytes + "-byte values");
        System.out.println();
    }

    private void printResults(List<Result> results) {
        System.out.println();
        System.out.println("| Workload | Throughput | p50 | p95 | p99 | max | Errors |");
        System.out.println("|---|---|---|---|---|---|---|");
        for (Result r : results) {
            System.out.printf("| %s | %,.0f ops/s | %.1f ms | %.1f ms | %.1f ms | %.1f ms | %d |%n",
                r.name(), r.throughput(), r.percentileMs(50), r.percentileMs(95), r.percentileMs(99), r.percentileMs(100), r.errors());
        }
    }

    private static void deleteRecursively(Path root) {
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
        }
    }

    // ------------------------------------------------------------------ an in-process cluster

    private static final class LocalCluster implements AutoCloseable {
        private final int size;
        private final Path root;
        private final int snapshotThreshold;
        private final Map<Integer, Integer> ports = new HashMap<>();
        private final Map<Integer, RaftNode> running = new ConcurrentHashMap<>();
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();

        LocalCluster(int size, Path root, int snapshotThreshold) throws IOException {
            this.size = size;
            this.root = root;
            this.snapshotThreshold = snapshotThreshold;
            for (int id = 1; id <= size; id++) ports.put(id, freePort());
        }

        void startAll() throws IOException {
            for (int id = 1; id <= size; id++) start(id);
        }

        void start(int id) throws IOException {
            Map<Integer, NodeConfig.PeerAddress> peers = new HashMap<>();
            for (int other = 1; other <= size; other++) {
                if (other != id) peers.put(other, new NodeConfig.PeerAddress(other, "localhost", ports.get(other)));
            }
            RaftNode node = new RaftNode(new NodeConfig(id, ports.get(id), root.resolve("node" + id), peers, snapshotThreshold));
            node.start();
            running.put(id, node);
        }

        void crash(int id) throws IOException {
            RaftNode node = running.remove(id);
            if (node != null) node.shutdown();
        }

        List<String> addresses() {
            List<String> list = new ArrayList<>();
            for (int id = 1; id <= size; id++) list.add("localhost:" + ports.get(id));
            return list;
        }

        Map<Integer, Long> syncCounts() {
            Map<Integer, Long> counts = new HashMap<>();
            running.forEach((id, node) -> counts.put(id, node.logSyncCount()));
            return counts;
        }

        boolean hasLeader() {
            return running.values().stream().anyMatch(n -> n.role() == NodeRole.LEADER);
        }

        RaftNode awaitLeader(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (RaftNode n : running.values()) if (n.role() == NodeRole.LEADER) return n;
                Thread.sleep(10);
            }
            throw new IllegalStateException("no leader within " + timeoutMs + " ms");
        }

        int anyFollower(int leaderId) {
            return running.keySet().stream().filter(id -> id != leaderId).findFirst().orElseThrow();
        }

        void awaitCommit(int id, long index, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                RaftNode n = running.get(id);
                if (n != null && n.commitIndex() >= index) return;
                Thread.sleep(5);
            }
            throw new IllegalStateException("node " + id + " did not reach commit index " + index);
        }

        void awaitCaughtUp(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                long max = running.values().stream().mapToLong(RaftNode::commitIndex).max().orElse(0);
                if (running.size() == size && running.values().stream().allMatch(n -> n.commitIndex() == max)) return;
                Thread.sleep(10);
            }
        }

        /** One short write attempt against whichever nodes are up; true if the cluster accepted it. */
        boolean tryWrite(String key) {
            for (int id : running.keySet()) {
                try {
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("key", key);
                    body.put("value", "v");
                    HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + ports.get(id) + "/client/set"))
                        .timeout(Duration.ofMillis(500))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(Json.encode(body)))
                        .build();
                    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                    if (ClientResponse.fromJson(response.body()).success()) return true;
                } catch (Exception ignored) {
                    // that node is down or not the leader; try the next
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            for (int id : new ArrayList<>(running.keySet())) crash(id);
        }

        // Below the ephemeral range, so outgoing connections cannot be handed a node's port.
        private static int freePort() throws IOException {
            for (int attempt = 0; attempt < 100; attempt++) {
                int port = 20_000 + ThreadLocalRandom.current().nextInt(20_000);
                try (ServerSocket s = new ServerSocket(port)) {
                    return port;
                } catch (IOException busy) {
                    // try another
                }
            }
            throw new IOException("no free port found");
        }
    }
}
