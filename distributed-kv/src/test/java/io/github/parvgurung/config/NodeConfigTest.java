package io.github.parvgurung.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NodeConfigTest {
    @Test
    void parsesValidArguments() {
        NodeConfig c = NodeConfig.fromArgs(new String[]{"--id=1", "--port=8001", "--peers=2:localhost:8002,3:localhost:8003"});
        assertEquals(1, c.nodeId);
        assertEquals(8001, c.port);
        assertEquals(3, c.clusterSize());
        assertEquals(2, c.majority());
    }

    @Test
    void rejectsZeroAndNegativeIds() {
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromArgs(new String[]{"--id=0", "--port=8001"}));
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromArgs(new String[]{"--id=-3", "--port=8001"}));
    }

    @Test
    void rejectsMalformedArgumentInsteadOfIgnoringIt() {
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromArgs(new String[]{"--id", "1", "--port=8001"}));
    }
}
