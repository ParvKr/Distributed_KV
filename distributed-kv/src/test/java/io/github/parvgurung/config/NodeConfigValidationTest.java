package io.github.parvgurung.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NodeConfigValidationTest {
    private static NodeConfig parse(String... args) {
        return NodeConfig.fromArgs(args);
    }

    @Test
    void peerSpecsAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=2:localhost"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=x:localhost:8002"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=2:localhost:80"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=2:localhost:70000"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=2:localhost:8002,2:localhost:8003"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--peers=1:localhost:8002"), "a node cannot be its own peer");
    }

    @Test
    void requiredArgumentsAreEnforced() {
        assertThrows(IllegalArgumentException.class, () -> parse("--port=8001"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=8001", "--unknown=1"));
    }

    @Test
    void portsMustBeUnprivilegedAndInRange() {
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=1023"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=65536"));
        assertThrows(IllegalArgumentException.class, () -> parse("--id=1", "--port=abc"));
        assertEquals(65535, parse("--id=1", "--port=65535").port);
        assertEquals(1024, parse("--id=1", "--port=1024").port);
    }

    @Test
    void emptyPeerListMeansASingleNodeCluster() {
        NodeConfig c = parse("--id=1", "--port=8001", "--peers=");
        assertEquals(1, c.clusterSize());
        assertEquals(1, c.majority());
    }

    @Test
    void majorityIsComputedForEvenAndOddClusterSizes() {
        assertEquals(1, parse("--id=1", "--port=8001").majority());
        assertEquals(2, parse("--id=1", "--port=8001", "--peers=2:h:8002").majority());                         // 2 nodes need both
        assertEquals(2, parse("--id=1", "--port=8001", "--peers=2:h:8002,3:h:8003").majority());                // 3 -> 2
        assertEquals(3, parse("--id=1", "--port=8001", "--peers=2:h:8002,3:h:8003,4:h:8004").majority());       // 4 -> 3
        assertEquals(3, parse("--id=1", "--port=8001", "--peers=2:h:8002,3:h:8003,4:h:8004,5:h:8005").majority()); // 5 -> 3
    }

    @Test
    void dataDirDefaultsPerNodeAndCanBeOverridden() {
        assertTrue(parse("--id=7", "--port=8001").dataDir.toString().endsWith("data/node7"));
        assertTrue(parse("--id=7", "--port=8001", "--dataDir=/tmp/custom").dataDir.toString().endsWith("custom"));
    }

    @Test
    void peerAddressesBuildBaseUrls() {
        NodeConfig c = parse("--id=1", "--port=8001", "--peers=2:example.org:9000");
        assertEquals("http://example.org:9000", c.peers.get(2).baseUrl());
    }
}
