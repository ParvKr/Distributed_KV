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

public final class KVClient {
    private static final int MAX_ATTEMPTS = 10;
    private static final long RETRY_BACKOFF_MS = 200;
    private final List<String> knownNodes;
    private final HttpClient http = HttpClient.newBuilder()
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
        ClientResponse response = executeWithRedirect(nodeUrl -> doGet(nodeUrl, key));
        return response.value();
    }

    public void set(String key, String value) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("value", value);
        executeWithRedirect(nodeUrl -> doPost(nodeUrl, "/client/set", body));
    }

    public void delete(String key) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        executeWithRedirect(nodeUrl -> doPost(nodeUrl, "/client/delete", body));
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
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
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
            } catch (IOException e) {
                lastError = e; // node unreachable -- move on to the next candidate
            }
        }
        throw new IOException("Could not reach the cluster leader after " + MAX_ATTEMPTS + " attempts"
            + (lastError != null ? " (last error: " + lastError.getMessage() + ")" : ""));
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
            return ClientResponse.fromJson(response.body());
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
        } catch (IOException e) {
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
            java -cp target/classes com.parv.raftkv.client.KVClient \\
            --nodes=localhost:8001,localhost:8002,localhost:8003 set foo bar
            """
        );
    }
}