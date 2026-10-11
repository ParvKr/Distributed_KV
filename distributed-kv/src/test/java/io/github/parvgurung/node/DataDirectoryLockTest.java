package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataDirectoryLockTest {
    @TempDir
    Path tempDir;

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private RaftNode node(Path dir, int port) throws IOException {
        return new RaftNode(new NodeConfig(1, port, dir, Map.of(), 0));
    }

    @Test
    void aSecondNodeOnTheSameDirectoryRefusesToStart() throws Exception {
        RaftNode first = node(tempDir, freePort());
        first.start();
        try {
            RaftNode second = node(tempDir, freePort());
            IOException e = assertThrows(IOException.class, second::start);
            assertTrue(e.getMessage().contains("already in use"), e.getMessage());
        } finally {
            first.shutdown();
        }
    }

    @Test
    void theDirectoryCanBeReusedAfterTheFirstNodeShutsDown() throws Exception {
        RaftNode first = node(tempDir, freePort());
        first.start();
        first.shutdown();
        RaftNode second = node(tempDir, freePort());
        second.start();
        second.shutdown();
    }

    @Test
    void aFailedStartReleasesTheLock() throws Exception {
        int port = freePort();
        RaftNode holder = node(tempDir.resolve("holder"), port);
        holder.start();
        try {
            Path dir = tempDir.resolve("victim");
            RaftNode blocked = node(dir, port);   // the port is taken, so start() fails after taking the lock
            assertThrows(IOException.class, blocked::start);
            RaftNode retry = node(dir, freePort());
            retry.start();                         // would throw "already in use" if the lock had leaked
            retry.shutdown();
        } finally {
            holder.shutdown();
        }
    }

    @Test
    void theLockFileDoesNotPreventNormalRestartsOfTheDataInside() throws Exception {
        RaftNode first = node(tempDir, freePort());
        first.start();
        first.shutdown();
        assertTrue(Files.exists(tempDir.resolve("node.lock")));
        RaftNode again = node(tempDir, freePort());
        again.start();
        again.shutdown();
    }
}
