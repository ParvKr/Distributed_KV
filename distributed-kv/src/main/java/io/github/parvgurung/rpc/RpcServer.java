package io.github.parvgurung.rpc;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import io.github.parvgurung.util.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer {
    private final int port;
    private final RaftRpcHandler handler;
    private HttpServer server;
    private ExecutorService executor;

    public RpcServer(int port, RaftRpcHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        executor = Executors.newFixedThreadPool(200);
        server.setExecutor(executor);
        server.createContext("/raft/requestVote", this::handleRequestVote);
        server.createContext("/raft/appendEntries", this::handleAppendEntries);
        server.createContext("/raft/installSnapshot", this::handleInstallSnapshot);
        server.createContext("/client/get", this::handleClientGet);
        server.createContext("/client/set", this::handleClientSet);
        server.createContext("/client/delete", this::handleClientDelete);
        server.createContext("/status", this::handleStatus);
        server.createContext("/status.json", this::handleStatusJson);
        server.start();
    }

    public void stop() {
        if (server != null)
            server.stop(0);
        if (executor != null)
            executor.shutdownNow();
    }

    private void handleRequestVote(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            RequestVoteRequest req = RequestVoteRequest.fromJson(body);
            RequestVoteResponse resp = handler.handleRequestVote(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleAppendEntries(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            AppendEntriesRequest req = AppendEntriesRequest.fromJson(body);
            AppendEntriesResponse resp = handler.handleAppendEntries(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleInstallSnapshot(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            InstallSnapshotRequest req = InstallSnapshotRequest.fromJson(body);
            InstallSnapshotResponse resp = handler.handleInstallSnapshot(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientGet(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String key = queryParam(exchange.getRequestURI(), "key");
            if (key == null || key.isEmpty())
                throw new IllegalArgumentException("Missing 'key' query parameter");
            ClientResponse resp = handler.handleClientGet(key);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientSet(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            Map<String, Object> obj = Json.decode(body);
            String key = Json.getString(obj, "key");
            String value = Json.getString(obj, "value");
            ClientResponse resp = handler.handleClientSet(key, value, Json.getString(obj, "clientId"), optionalSeq(obj));
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientDelete(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            Map<String, Object> obj = Json.decode(body);
            String key = Json.getString(obj, "key");
            ClientResponse resp = handler.handleClientDelete(key, Json.getString(obj, "clientId"), optionalSeq(obj));
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private static long optionalSeq(Map<String, Object> obj) {
        return obj.get("seq") == null ? 0 : Json.getLong(obj, "seq");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String summary = handler.statusSummary();
            byte[] bytes = summary.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
    }

    private void handleStatusJson(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            // Read-only and carries no secrets, so a dashboard served from elsewhere may poll it.
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            writeJson(exchange, 200, handler.statusJson());
        });
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private void withErrorHandling(HttpExchange exchange, ThrowingRunnable action) throws IOException {
        try {
            action.run();
        } catch(IllegalArgumentException e) {
            // malformed JSON, missing or invalid fields: the caller's fault
            writeError(exchange, 400, e);
        } catch(Exception e) {
            writeError(exchange, 500, e);
        } finally {
            exchange.close();
        }
    }

    private void writeError(HttpExchange exchange, int status, Exception e) throws IOException {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        writeJson(exchange, status, ClientResponse.error(message).toJson());
    }

    private String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private void writeJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) return null;
        for (String pair : query.split("&")) {
            var kv = pair.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) {
                return java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}