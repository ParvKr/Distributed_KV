package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.rpc.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Browser access to the client endpoints: only for the cluster's own nodes and an explicitly configured
 * origin, and writes must be JSON POSTs so other sites cannot submit them without a preflight.
 */
class ClientEndpointSecurityTest {
    private static final int PEER_PORT = 9;   // nothing listens here
    private static final String ELSEWHERE = "https://dash.example.com";
    private static final String STRANGER = "https://evil.example.net";

    @TempDir
    Path tempDir;

    private final HttpClient http = HttpClient.newHttpClient();
    private RaftNode node;
    private int port;

    @AfterEach
    void stop() throws IOException {
        if (node != null) node.shutdown();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A one-node cluster: it elects itself leader, so writes work. */
    private void startSingleNode(String allowOrigin) throws Exception {
        port = freePort();
        node = new RaftNode(new NodeConfig(1, port, tempDir, Map.of(), 0, allowOrigin));
        node.start();
        long deadline = System.currentTimeMillis() + 5000;
        while (node.role() != NodeRole.LEADER && System.currentTimeMillis() < deadline) Thread.sleep(25);
        assertEquals(NodeRole.LEADER, node.role());
    }

    /** A node with a (silent) peer, so a peer origin exists to be trusted. It never becomes leader. */
    private void startNodeWithPeer(String allowOrigin) throws Exception {
        port = freePort();
        node = new RaftNode(new NodeConfig(1, port, tempDir,
            Map.of(2, new NodeConfig.PeerAddress(2, "localhost", PEER_PORT)), 0, allowOrigin));
        node.start();
    }

    private HttpResponse<String> send(String method, String path, String contentType, String body, String origin) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path));
        if (contentType != null) b.header("Content-Type", contentType);
        if (origin != null) b.header("Origin", origin);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String allowedOrigin(HttpResponse<String> r) {
        return r.headers().firstValue("Access-Control-Allow-Origin").orElse(null);
    }

    // ------------------------------------------------------------------ who gets browser access

    @Test
    void strangersGetNoCorsAccessByDefault() throws Exception {
        startNodeWithPeer(null);
        assertNull(allowedOrigin(send("GET", "/client/get?key=k", null, null, STRANGER)));
        HttpResponse<String> pre = send("OPTIONS", "/client/set", null, null, STRANGER);
        assertNull(allowedOrigin(pre));
        assertNotEquals(204, pre.statusCode());
    }

    @Test
    void requestsWithoutAnOriginGetNoCorsHeaders() throws Exception {
        startNodeWithPeer(null);
        assertNull(allowedOrigin(send("GET", "/client/get?key=k", null, null, null)));
    }

    @Test
    void thePagesOfPeerNodesAreTrustedAutomatically() throws Exception {
        startNodeWithPeer(null);
        for (String peerOrigin : new String[]{"http://localhost:" + PEER_PORT, "http://127.0.0.1:" + PEER_PORT}) {
            HttpResponse<String> pre = send("OPTIONS", "/client/set", null, null, peerOrigin);
            assertEquals(204, pre.statusCode(), peerOrigin);
            assertEquals(peerOrigin, allowedOrigin(pre));
            assertTrue(pre.headers().firstValue("Access-Control-Allow-Headers").orElse("").contains("Content-Type"));

            HttpResponse<String> get = send("GET", "/client/get?key=k", null, null, peerOrigin);
            assertEquals(peerOrigin, allowedOrigin(get));
            assertEquals("Origin", get.headers().firstValue("Vary").orElse(null));
        }
    }

    @Test
    void aConfiguredOriginIsAllowedButOthersStillAreNot() throws Exception {
        startNodeWithPeer(ELSEWHERE);

        HttpResponse<String> pre = send("OPTIONS", "/client/set", null, null, ELSEWHERE);
        assertEquals(204, pre.statusCode());
        assertEquals(ELSEWHERE, allowedOrigin(pre));

        assertNull(allowedOrigin(send("GET", "/client/get?key=k", null, null, STRANGER)));
    }

    @Test
    void wildcardAllowsAnyOrigin() throws Exception {
        startNodeWithPeer("*");
        assertEquals("*", allowedOrigin(send("GET", "/client/get?key=k", null, null, STRANGER)));
        assertEquals("*", allowedOrigin(send("GET", "/client/get?key=k", null, null, null)));
    }

    // ------------------------------------------------------------------ what a write must look like

    @Test
    void writesWithoutAJsonContentTypeAreRejectedAndNotApplied() throws Exception {
        startSingleNode(null);
        String body = "{\"key\":\"k\",\"value\":\"v\"}";

        assertEquals(415, send("POST", "/client/set", "text/plain", body, STRANGER).statusCode());
        assertEquals(415, send("POST", "/client/set", "application/x-www-form-urlencoded", body, STRANGER).statusCode());
        assertEquals(415, send("POST", "/client/delete", null, "{\"key\":\"k\"}", null).statusCode());

        HttpResponse<String> get = send("GET", "/client/get?key=k", null, null, null);
        assertNull(ClientResponse.fromJson(get.body()).value(), "a rejected write must not have been applied");
    }

    @Test
    void jsonWritesWithACharsetParameterStillWork() throws Exception {
        startSingleNode(null);
        HttpResponse<String> r = send("POST", "/client/set", "application/json; charset=UTF-8", "{\"key\":\"k\",\"value\":\"v\"}", null);
        assertEquals(200, r.statusCode());
        assertTrue(ClientResponse.fromJson(r.body()).success());
    }

    @Test
    void wrongMethodsAreRejected() throws Exception {
        startSingleNode(null);
        assertEquals(405, send("GET", "/client/set", null, null, null).statusCode());
        assertEquals(405, send("POST", "/client/get?key=k", "application/json", "{}", null).statusCode());
    }

    // ------------------------------------------------------------------ configuration

    @Test
    void invalidAllowOriginIsRejectedAtConfigTime() {
        assertThrows(IllegalArgumentException.class,
            () -> NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001", "--allowOrigin=example.com"}));
        assertThrows(IllegalArgumentException.class,
            () -> NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001", "--allowOrigin=https://example.com/path"}));
        assertEquals("https://example.com",
            NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001", "--allowOrigin=https://example.com"}).allowOrigin);
        assertEquals("*", NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001", "--allowOrigin=*"}).allowOrigin);
        assertNull(NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001"}).allowOrigin);
    }
}
