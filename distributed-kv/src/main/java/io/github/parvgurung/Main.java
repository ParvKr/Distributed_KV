package io.github.parvgurung;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.node.RaftNode;;

public final class Main {
    public static void main(String[] args) {
        final NodeConfig config;
        try {
            config = NodeConfig.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid arguments: " + e.getMessage());
            System.exit(1);
            return;
        }
        System.out.println("Starting raftkv node with config: " + config);
        RaftNode node = new RaftNode(config);
        try {
            node.start();
        } catch (Exception e) {
            System.err.println("Failed to start node: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down node " + config.nodeId + "...");
            try {
                node.shutdown();
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }, "shutdown-hook"));
        System.out.println("Node " + config.nodeId + " is up. Listening on port " + config.port + ".");
        System.out.println("Status endpoint: http://localhost:" + config.port + "/status");
        System.out.println("Press Ctrl+C to stop.");
        try {
            Thread.currentThread().join(); // keep the process alive; shutdown hook handles Ctrl+C
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}