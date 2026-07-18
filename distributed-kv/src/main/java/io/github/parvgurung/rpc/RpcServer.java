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
import java.util.concurrent.Executors;

public class RpcServer {
    private final int port;
    private final RaftRpcHandler handler;
    private HttpServer server;

    public RpcServer(int port, RaftRpcHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(200));
        server.createContext("/raft/requestVote", this::handleRequestVote);
        server.createContext("/raft/appendEntries", this::handleAppendEntries);
        server.createContext("/client/get", this::handleClientGet);
        server.createContext("/client/set", this::handleClientSet);
        server.createContext("/client/delete", this::handleClientDelete);
        server.createContext("/status", this::handleStatus);
        server.start();
    }

    public void stop() {
        if (server != null)
            server.stop(0);
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

    private void handleClientGet(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String key = queryParam(exchange.getRequestURI(), "key");
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
            ClientResponse resp = handler.handleClientSet(key, value);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientDelete(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            String body = readBody(exchange);
            Map<String, Object> obj = Json.decode(body);
            String key = Json.getString(obj, "key");
            ClientResponse resp = handler.handleClientDelete(key);
            writeJson(exchange, 200, resp.toJson());
        });
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

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private void withErrorHandling(HttpExchange exchange, ThrowingRunnable action) throws IOException {
        try {
            action.run();
        } catch(Exception e) {
            String msg = "{\"error\":\"" + e.getMessage() + "\"}";
            byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(500, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } finally {
            exchange.close();
        }
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