package io.github.parvgurung.client;

import io.github.parvgurung.rpc.ClientResponse;
import io.github.parvgurung.util.Json;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class KVClient {
    // Long enough to ride out an election (election timeouts are 0.8-1.5s).
    private static final long RETRY_WINDOW_MS = 10_000;
    private static final long RETRY_BACKOFF_MS = 200;
    private final List<String> knownNodes;
    private final HttpClient http = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofMillis(500))
        .build();
    private volatile String cachedLeader;
    public KVClient(List<String> knownNodes) {
        if (knownNodes.isEmpty()) {
            throw new IllegalArgumentException("Need at least one node address");
        }
        this.knownNodes = knownNodes;
    }

    public String get(String key) throws IOException {
        requireKey(key);
        ClientResponse response = executeWithRedirect(nodeUrl -> doGet(nodeUrl, key));
        return response.value();
    }

    public void set(String key, String value) throws IOException {
        requireKey(key);
        if (value == null) throw new IllegalArgumentException("Value cannot be null");
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("value", value);
        executeWithRedirect(nodeUrl -> doPost(nodeUrl, "/client/set", body));
    }

    public void delete(String key) throws IOException {
        requireKey(key);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        executeWithRedirect(nodeUrl -> doPost(nodeUrl, "/client/delete", body));
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("Key cannot be null or empty");
    }

    private interface Request {
        ClientResponse call(String nodeUrl) throws IOException;
    }

    private ClientResponse executeWithRedirect(Request request) throws IOException {
        Deque<String> toTry = new ArrayDeque<>();
        if (cachedLeader != null) toTry.add(cachedLeader);
        for (String node : knownNodes) {
            if (!toTry.contains(node)) toTry.add(node);
        }
        IOException lastError = null;
        String lastServerError = null;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_WINDOW_MS);
        while (System.nanoTime() < deadline) {
            if (toTry.isEmpty()) {
                sleep(RETRY_BACKOFF_MS);
                for (String node : knownNodes) toTry.add(node);
            }
            String node = toTry.poll();
            try {
                ClientResponse response = request.call("http://" + node);
                if (response.success()) {
                    cachedLeader = node;
                    return response;
                }
                if (response.leaderHint() != null && !response.leaderHint().equals(node)) {
                    cachedLeader = response.leaderHint();
                    toTry.addFirst(cachedLeader);
                    continue;
                }
                if (!response.isNotLeader() && response.error() != null)
                    lastServerError = response.error();
            } catch (IOException e) {
                lastError = e; // node unreachable -- move on to the next candidate
            }
        }
        throw new IOException("Could not complete the request within " + RETRY_WINDOW_MS + "ms"
            + (lastServerError != null ? " (last server error: " + lastServerError + ")" : "")
            + (lastError != null ? " (last connection error: " + lastError.getMessage() + ")" : ""));
    }  
    
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ClientResponse doGet(String nodeUrl, String key) throws IOException {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(URI.create(nodeUrl + "/client/get?key=" + encodedKey))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();
        return send(request);
    }

    private ClientResponse doPost(String nodeUrl, String path, Map<String, Object> body) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(nodeUrl + path))
            .timeout(Duration.ofSeconds(3))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(Json.encode(body)))
            .build();
        return send(request);
    }

    private ClientResponse send(HttpRequest request) throws IOException {
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            ClientResponse parsed = ClientResponse.fromJson(response.body());
            if (response.statusCode() == 400)
                throw new IllegalArgumentException(parsed.error()); // our request is bad; retrying can't help
            return parsed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for response", e);
        }
    }

    public static void main(String[] args) {
        String nodesArg = null;
        List<String> rest = new ArrayList<>();
        for (String arg : args) {
            if (arg.startsWith("--nodes=")) {
                nodesArg = arg.substring("--nodes=".length());
            } else {
            rest.add(arg);
            }
        }
        if (nodesArg == null || rest.isEmpty()) {
            printUsage();
            System.exit(1);
            return;
        }
        List<String> nodes = Arrays.asList(nodesArg.split(","));
        KVClient client = new KVClient(nodes);
        try {
            String command = rest.get(0).toLowerCase();
            switch (command) {
                case "get" -> {
                    requireArgs(rest, 2, "get <key>");
                    String value = client.get(rest.get(1));
                    System.out.println(value == null ? "(nil)" : value);
                }
                case "set" -> {
                    requireArgs(rest, 3, "set <key> <value>");
                    client.set(rest.get(1), rest.get(2));
                    System.out.println("OK");
                }
                case "delete" -> {
                    requireArgs(rest, 2, "delete <key>");
                    client.delete(rest.get(1));
                    System.out.println("OK");
                }
                default -> {
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void requireArgs(List<String> rest, int minSize, String usage) {
        if (rest.size() < minSize) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("""
            Usage:
            --nodes=host:port,host:port,... get <key>
            --nodes=host:port,host:port,... set <key> <value>
            --nodes=host:port,host:port,... delete <key>
            Example:
            java -cp target/classes io.github.parvgurung.client.KVClient \\
            --nodes=localhost:8001,localhost:8002,localhost:8003 set foo bar
            """
        );
    }
}