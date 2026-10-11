package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.rpc.ClientResponse;
import io.github.parvgurung.util.Json;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

/** A cluster of in-process nodes with crash/restart and network-partition controls, for scenario tests. */
final class TestCluster implements AutoCloseable {
    final int size;
    private final Path root;
    private final int snapshotThreshold;
    private final Map<Integer, Integer> ports = new HashMap<>();
    private final Map<Integer, NodeConfig> configs = new HashMap<>();
    private final Map<Integer, PartitionableRpcClient> clients = new ConcurrentHashMap<>();
    private final Map<Integer, RaftNode> running = new ConcurrentHashMap<>();
    private final Set<Integer> isolated = ConcurrentHashMap.newKeySet();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    TestCluster(int size, Path root, int snapshotThreshold) throws IOException {
        this.size = size;
        this.root = root;
        this.snapshotThreshold = snapshotThreshold;
        for (int id = 1; id <= size; id++) ports.put(id, freePort());
        for (int id = 1; id <= size; id++) {
            Map<Integer, NodeConfig.PeerAddress> peers = new HashMap<>();
            for (int other = 1; other <= size; other++) {
                if (other != id) peers.put(other, new NodeConfig.PeerAddress(other, "localhost", ports.get(other)));
            }
            configs.put(id, new NodeConfig(id, ports.get(id), root.resolve("node" + id), peers, snapshotThreshold));
        }
    }

    // ------------------------------------------------------------------ lifecycle

    void startAll() throws IOException {
        for (int id = 1; id <= size; id++) start(id);
    }

    void start(int id) throws IOException {
        PartitionableRpcClient client = new PartitionableRpcClient();
        clients.put(id, client);
        RaftNode node = new RaftNode(configs.get(id), client);
        node.start();
        running.put(id, node);
        applyPartitions();
    }

    void crash(int id) throws IOException {
        RaftNode node = running.remove(id);
        if (node != null) node.shutdown();
    }

    boolean isRunning(int id) {
        return running.containsKey(id);
    }

    RaftNode node(int id) {
        return running.get(id);
    }

    List<RaftNode> runningNodes() {
        return new ArrayList<>(running.values());
    }

    Path dataDir(int id) {
        return root.resolve("node" + id);
    }

    int port(int id) {
        return ports.get(id);
    }

    @Override
    public void close() throws IOException {
        for (int id : new ArrayList<>(running.keySet())) crash(id);
    }

    // ------------------------------------------------------------------ partitions

    void isolate(int id) {
        isolated.add(id);
        applyPartitions();
    }

    void heal() {
        isolated.clear();
        applyPartitions();
    }

    private void applyPartitions() {
        for (int from = 1; from <= size; from++) {
            PartitionableRpcClient client = clients.get(from);
            if (client == null) continue;
            Set<String> blockedUrls = new HashSet<>();
            for (int to = 1; to <= size; to++) {
                if (to != from && (isolated.contains(from) || isolated.contains(to)))
                    blockedUrls.add("http://localhost:" + ports.get(to));
            }
            client.setBlocked(blockedUrls);
        }
    }

    // ------------------------------------------------------------------ cluster state

    /** The running node that currently believes it is leader in the highest term, if any. */
    Optional<RaftNode> leader() {
        RaftNode best = null;
        long bestTerm = 0;
        for (RaftNode n : running.values()) {
            long t = n.termIfLeader();
            if (t > bestTerm) { best = n; bestTerm = t; }
        }
        return Optional.ofNullable(best);
    }

    RaftNode awaitLeader(long timeoutMs) throws InterruptedException {
        RaftNode[] found = new RaftNode[1];
        awaitCondition("a leader", timeoutMs, () -> {
            Optional<RaftNode> l = leader();
            l.ifPresent(n -> found[0] = n);
            return l.isPresent();
        });
        return found[0];
    }

    /** Waits for a leader that is not one of the given nodes. */
    RaftNode awaitLeaderOtherThan(long timeoutMs, int... excluded) throws InterruptedException {
        Set<Integer> skip = new HashSet<>();
        for (int e : excluded) skip.add(e);
        RaftNode[] found = new RaftNode[1];
        awaitCondition("a leader other than " + skip, timeoutMs, () -> {
            for (RaftNode n : running.values()) {
                if (!skip.contains(n.nodeId()) && n.termIfLeader() > 0) { found[0] = n; return true; }
            }
            return false;
        });
        return found[0];
    }

    void awaitCondition(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(25);
        }
        throw new AssertionError("timed out after " + timeoutMs + "ms waiting for " + what + "\n" + describe());
    }

    String describe() {
        StringBuilder sb = new StringBuilder("cluster (isolated=" + isolated + ", running=" + new TreeSet<>(running.keySet()) + "):");
        for (RaftNode n : new TreeMap<>(running).values()) sb.append("\n  ").append(n.statusSummary());
        return sb.toString();
    }

    /** True when every running node has applied the same, fully committed state. */
    boolean converged() {
        KVStateMachineView first = null;
        for (RaftNode n : running.values()) {
            KVStateMachineView v = new KVStateMachineView(n);
            if (v.commit != v.state.lastAppliedIndex()) return false;
            if (first == null) first = v;
            else if (v.commit != first.commit || !v.state.data().equals(first.state.data())) return false;
        }
        return true;
    }

    private static final class KVStateMachineView {
        final long commit;
        final io.github.parvgurung.statemachine.KVStateMachine.State state;
        KVStateMachineView(RaftNode n) {
            // both read under the node lock one after the other; a mismatch just means "not converged yet"
            this.state = n.stateSnapshot();
            this.commit = n.commitIndex();
        }
    }

    // ------------------------------------------------------------------ client calls

    ClientResponse set(RaftNode node, String key, String value, String clientId, long seq) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("value", value);
        if (clientId != null) { body.put("clientId", clientId); body.put("seq", seq); }
        return post(node, "/client/set", Json.encode(body), Duration.ofSeconds(4));
    }

    ClientResponse delete(RaftNode node, String key) throws Exception {
        return post(node, "/client/delete", Json.encode(Map.of("key", key)), Duration.ofSeconds(4));
    }

    ClientResponse get(RaftNode node, String key) throws Exception {
        String q = URLEncoder.encode(key, StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(uri(node.nodeId(), "/client/get?key=" + q)).timeout(Duration.ofSeconds(4)).GET().build();
        return ClientResponse.fromJson(http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body());
    }

    ClientResponse post(RaftNode node, String path, String jsonBody, Duration timeout) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri(node.nodeId(), path))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build();
        return ClientResponse.fromJson(http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body());
    }

    /** Raw HTTP, for protocol robustness tests. */
    HttpResponse<String> raw(int nodeId, String method, String path, String contentType, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri(nodeId, path)).timeout(Duration.ofSeconds(10));
        if (contentType != null) b.header("Content-Type", contentType);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    /** Sends one raw HTTP/1.1 request line (for URLs the HTTP client library refuses to build) and returns the status code. */
    int rawStatus(int nodeId, String requestLine) throws IOException {
        try (java.net.Socket socket = new java.net.Socket("localhost", ports.get(nodeId))) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write((requestLine + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .getBytes(StandardCharsets.ISO_8859_1));
            socket.getOutputStream().flush();
            String statusLine = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1)).readLine();
            if (statusLine == null) throw new IOException("connection closed without a response");
            return Integer.parseInt(statusLine.split(" ")[1]);
        }
    }

    private URI uri(int nodeId, String path) {
        return URI.create("http://localhost:" + ports.get(nodeId) + path);
    }

    private static int freePort() throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int port = 20_000 + ThreadLocalRandom.current().nextInt(20_000);
            try (ServerSocket s = new ServerSocket(port)) {
                return port;
            } catch (IOException busy) {
                // try another
            }
        }
        throw new IOException("no free port");
    }
}
