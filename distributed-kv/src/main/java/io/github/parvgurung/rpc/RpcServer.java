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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer {
    private final int port;
    private final RaftRpcHandler handler;
    private HttpServer server;
    private ExecutorService executor;

    private final Set<String> allowedOrigins;   // from --allowOrigin; may contain "*"
    private final Set<String> trustedOrigins;

    public RpcServer(int port, RaftRpcHandler handler) {
        this(port, handler, null, Set.of());
    }

    /**
     * @param allowOrigin    a web origin (or "*") browsers may call the client endpoints from; null for none
     * @param trustedOrigins further origins allowed to do the same, e.g. the other cluster nodes
     */
    public RpcServer(int port, RaftRpcHandler handler, String allowOrigin, Set<String> trustedOrigins) {
        this.port = port;
        this.handler = handler;
        Set<String> allowed = new java.util.HashSet<>();
        if (allowOrigin != null) {
            for (String origin : allowOrigin.split(",")) allowed.add(origin.trim());
        }
        this.allowedOrigins = Set.copyOf(allowed);
        this.trustedOrigins = Set.copyOf(trustedOrigins);
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        executor = Executors.newFixedThreadPool(200);
        server.setExecutor(executor);
        server.createContext("/raft/requestVote", this::handleRequestVote);
        server.createContext("/raft/appendEntries", this::handleAppendEntries);
        server.createContext("/raft/installSnapshot", this::handleInstallSnapshot);
        server.createContext("/client/get", ex -> withClientCors(ex, this::handleClientGet));
        server.createContext("/client/set", ex -> withClientCors(ex, this::handleClientSet));
        server.createContext("/client/delete", ex -> withClientCors(ex, this::handleClientDelete));
        server.createContext("/status", this::handleStatus);
        server.createContext("/status.json", this::handleStatusJson);
        server.createContext("/dashboard", this::handleDashboard);
        server.createContext("/", this::handleRoot);
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
            if (rejectUnlessJsonPost(exchange)) return;
            String body = readBody(exchange, MAX_RAFT_BODY_BYTES);
            RequestVoteRequest req = RequestVoteRequest.fromJson(body);
            RequestVoteResponse resp = handler.handleRequestVote(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleAppendEntries(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (rejectUnlessJsonPost(exchange)) return;
            String body = readBody(exchange, MAX_RAFT_BODY_BYTES);
            AppendEntriesRequest req = AppendEntriesRequest.fromJson(body);
            AppendEntriesResponse resp = handler.handleAppendEntries(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleInstallSnapshot(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (rejectUnlessJsonPost(exchange)) return;
            String body = readBody(exchange, MAX_SNAPSHOT_BODY_BYTES);
            InstallSnapshotRequest req = InstallSnapshotRequest.fromJson(body);
            InstallSnapshotResponse resp = handler.handleInstallSnapshot(req);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }

    /**
     * Lets a browser page from the configured origin call the client endpoints, and answers its preflight
     * check. With no origin configured no CORS headers are sent, so browsers keep other sites out.
     */
    private void withClientCors(HttpExchange exchange, ExchangeHandler next) throws IOException {
        String allowed = corsOriginFor(exchange.getRequestHeaders().getFirst("Origin"));
        if (allowed != null) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", allowed);
            if (!allowed.equals("*"))
                exchange.getResponseHeaders().add("Vary", "Origin");
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                exchange.getResponseHeaders().set("Access-Control-Max-Age", "600");
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
        }
        next.handle(exchange);
    }

    /** The value for Access-Control-Allow-Origin, or null if a page from {@code origin} gets no access. */
    private String corsOriginFor(String origin) {
        if (allowedOrigins.contains("*"))
            return "*";
        if (origin != null && (allowedOrigins.contains(origin) || trustedOrigins.contains(origin)))
            return origin;
        return null;
    }

    /**
     * Writes must be JSON POSTs. Requiring that content type means a web page on another site cannot
     * submit a write (a plain form or text/plain request) without first passing the CORS preflight.
     */
    private boolean rejectUnlessJsonPost(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            writeJson(exchange, 405, ClientResponse.error("Use POST").toJson());
            return true;
        }
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.toLowerCase().startsWith("application/json")) {
            writeJson(exchange, 415, ClientResponse.error("Content-Type must be application/json").toJson());
            return true;
        }
        return false;
    }

    private void handleClientGet(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                writeJson(exchange, 405, ClientResponse.error("Use GET").toJson());
                return;
            }
            String key = queryParam(exchange.getRequestURI(), "key");
            if (key == null || key.isEmpty())
                throw new IllegalArgumentException("Missing 'key' query parameter");
            ClientResponse resp = handler.handleClientGet(key);
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientSet(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (rejectUnlessJsonPost(exchange)) return;
            String body = readBody(exchange, MAX_CLIENT_BODY_BYTES);
            Map<String, Object> obj = Json.decode(body);
            String key = Json.getString(obj, "key");
            String value = Json.getString(obj, "value");
            ClientResponse resp = handler.handleClientSet(key, value, Json.getString(obj, "clientId"), optionalSeq(obj));
            writeJson(exchange, 200, resp.toJson());
        });
    }

    private void handleClientDelete(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (rejectUnlessJsonPost(exchange)) return;
            String body = readBody(exchange, MAX_CLIENT_BODY_BYTES);
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

    private void handleDashboard(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            try (var page = RpcServer.class.getResourceAsStream("/dashboard/index.html")) {
                if (page == null)
                    throw new IOException("dashboard page is missing from the build");
                byte[] bytes = page.readAllBytes();
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
    }

    // Only "/" itself goes to the dashboard; every other unknown path is a plain 404.
    private void handleRoot(HttpExchange exchange) throws IOException {
        withErrorHandling(exchange, () -> {
            if (exchange.getRequestURI().getPath().equals("/")) {
                exchange.getResponseHeaders().set("Location", "/dashboard");
                exchange.sendResponseHeaders(302, -1);
            } else {
                exchange.sendResponseHeaders(404, -1);
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
        } catch(PayloadTooLargeException e) {
            writeError(exchange, 413, e);
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

    // Generous limits: they exist so one request cannot make a node read gigabytes, not to police normal use.
    private static final int MAX_CLIENT_BODY_BYTES = 16 * 1024 * 1024;
    private static final int MAX_RAFT_BODY_BYTES = 128 * 1024 * 1024;
    private static final int MAX_SNAPSHOT_BODY_BYTES = 512 * 1024 * 1024;

    private static final class PayloadTooLargeException extends RuntimeException {
        PayloadTooLargeException(int limit) {
            super("Request body is larger than the " + (limit / (1024 * 1024)) + " MiB limit");
        }
    }

    private String readBody(HttpExchange exchange, int limit) throws IOException {
        String declared = exchange.getRequestHeaders().getFirst("Content-Length");
        if (declared != null) {
            try {
                if (Long.parseLong(declared.trim()) > limit) throw new PayloadTooLargeException(limit);
            } catch (NumberFormatException ignored) {
                // a bogus header is the HTTP layer's problem; the byte limit below still applies
            }
        }
        byte[] bytes = exchange.getRequestBody().readNBytes(limit + 1);
        if (bytes.length > limit) throw new PayloadTooLargeException(limit);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void writeJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    /** Percent-decoding that rejects invalid UTF-8 instead of quietly substituting U+FFFD (which would merge distinct keys). */
    private static String decodeStrictly(String raw) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < raw.length(); ) {
            int cp = raw.codePointAt(i);
            if (cp == '%') {
                if (i + 2 >= raw.length())
                    throw new IllegalArgumentException("Truncated percent-escape in query string");
                try {
                    bytes.write(Integer.parseInt(raw.substring(i + 1, i + 3), 16));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid percent-escape in query string");
                }
                i += 3;
            } else if (cp == '+') {
                bytes.write(' ');
                i++;
            } else {
                byte[] utf8 = new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8);
                bytes.write(utf8, 0, utf8.length);
                i += Character.charCount(cp);
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                .decode(java.nio.ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new IllegalArgumentException("Query string is not valid UTF-8");
        }
    }

    private String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) return null;
        for (String pair : query.split("&")) {
            var kv = pair.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) {
                return decodeStrictly(kv[1]);
            }
        }
        return null;
    }
}