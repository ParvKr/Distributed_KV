package io.github.parvgurung.config;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class NodeConfig {
    public final int nodeId;
    public final int port;
    public final Path dataDir;

    public final Map<Integer, PeerAddress> peers;

    /** Take a snapshot once this many entries have been applied since the last one; 0 disables snapshots. */
    public final int snapshotThreshold;

    public static final int DEFAULT_SNAPSHOT_THRESHOLD = 1000;

    /**
     * Web origins (comma separated, or "*") allowed to call the client endpoints from a browser, or null to
     * allow none. Nodes already trust their peers' origins; this is for a page hosted somewhere else or
     * reached under a different name (e.g. through Docker port mappings).
     */
    public final String allowOrigin;

    public NodeConfig(int nodeId, int port, Path dataDir, Map<Integer, PeerAddress> peers) {
        this(nodeId, port, dataDir, peers, DEFAULT_SNAPSHOT_THRESHOLD);
    }

    public NodeConfig(int nodeId, int port, Path dataDir, Map<Integer, PeerAddress> peers, int snapshotThreshold) {
        this(nodeId, port, dataDir, peers, snapshotThreshold, null);
    }

    public NodeConfig(int nodeId, int port, Path dataDir, Map<Integer, PeerAddress> peers, int snapshotThreshold, String allowOrigin) {
        if (snapshotThreshold < 0)
            throw new IllegalArgumentException("snapshotThreshold cannot be negative");
        if (allowOrigin != null) {
            for (String origin : allowOrigin.split(",", -1)) {
                String o = origin.trim();
                if (!o.equals("*") && !o.matches("https?://[^/\\s,]+"))
                    throw new IllegalArgumentException("allowOrigin must be \"*\" or origins like https://example.com (no path), "
                        + "separated by commas, but got: " + allowOrigin);
            }
        }
        this.allowOrigin = allowOrigin;
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = dataDir;
        this.peers = Map.copyOf(peers);
        this.snapshotThreshold = snapshotThreshold;
    }

    /**
     * Web origins of the other cluster nodes. A dashboard page served by one node calls the others from
     * the browser, so each node trusts its peers' origins (and the loopback spellings of them).
     */
    public Set<String> trustedBrowserOrigins() {
        Set<String> origins = new LinkedHashSet<>();
        for (PeerAddress peer : peers.values()) {
            origins.add("http://" + peer.host() + ":" + peer.port());
            if (peer.host().equals("localhost") || peer.host().equals("127.0.0.1") || peer.host().equals("[::1]")) {
                origins.add("http://localhost:" + peer.port());
                origins.add("http://127.0.0.1:" + peer.port());
                origins.add("http://[::1]:" + peer.port());
            }
        }
        return origins;
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    public int majority() {
        return (clusterSize() / 2) + 1;
    }

    private static int parsePositiveInt(String value, String field) {
        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        }
        catch(NumberFormatException e) {
            throw new IllegalArgumentException(field + " must be a positive integer, but got: " + value);
        }
        if(parsed <= 0)
            throw new IllegalArgumentException(field + " must be a positive integer, but got: " + value);
        return parsed;
    }

    public static NodeConfig fromArgs(String[] args) {
        Integer id = null;
        Integer port = null;
        Path dataDir = null;
        int snapshotThreshold = DEFAULT_SNAPSHOT_THRESHOLD;
        String allowOrigin = null;
        Map<Integer, PeerAddress> peers = new HashMap<>();

        for(String arg: args) {
            if(!arg.startsWith("--") || !arg.contains("="))
                throw new IllegalArgumentException("Malformed argument: " + arg + ". Expected --name=value");
            var kv = arg.substring(2).split("=", 2);
            String key = kv[0];
            String value = kv[1];
            switch(key) {
                case "id" -> {
                    id = parsePositiveInt(value, "ID");
                }
                case "port" -> {
                    port = parsePositiveInt(value, "Port");
                    if(port < 1024 || port > 65535)
                        throw new IllegalArgumentException("Port must be in the range 1024-65535");
                }
                case "dataDir" -> dataDir = Path.of(value).toAbsolutePath();
                case "allowOrigin" -> allowOrigin = value;
                case "snapshotThreshold" -> {
                    try {
                        snapshotThreshold = Integer.parseInt(value);
                    } catch(NumberFormatException e) {
                        throw new IllegalArgumentException("snapshotThreshold must be a non-negative integer, but got: " + value);
                    }
                    if(snapshotThreshold < 0)
                        throw new IllegalArgumentException("snapshotThreshold must be a non-negative integer, but got: " + value);
                }
                case "peers" -> {
                    if(!value.isBlank()) {
                        for(String peerSpec: value.split(",")) {
                            // format: <id>:<host>:<port>
                            var parts = peerSpec.split(":", 3);
                            if(parts.length != 3)
                                throw new IllegalArgumentException("Invalid peer format: " + peerSpec + ". Expected format: <id>:<host>:<port>");
                            
                            int peerId = parsePositiveInt(parts[0], "Peer ID");
                            if(peers.containsKey(peerId))
                                throw new IllegalArgumentException("Duplicate peer ID: " + peerId);
                            
                            int peerPort = parsePositiveInt(parts[2], "Peer Port");
                            if(peerPort < 1024 || peerPort > 65535)
                                throw new IllegalArgumentException("Peer port must be in the range 1024-65535: " + peerSpec);
                            
                            peers.put(peerId, new PeerAddress(peerId, parts[1], peerPort));
                        }
                    }
                }
                default -> { 
                    throw new IllegalArgumentException("Unknown argument: " + key);
                }
            }
        }

        if(id == null) 
            throw new IllegalArgumentException("Missing required arguments: --id");
        if(port == null)
            throw new IllegalArgumentException("Missing required arguments: --port");
        if(dataDir == null)
            dataDir = Path.of("./data/node" + id).toAbsolutePath();
        
        if(peers.containsKey(id))
            throw new IllegalArgumentException("Node ID cannot be the same as any peer ID: " + id);
        return new NodeConfig(id, port, dataDir, peers, snapshotThreshold, allowOrigin);
    }

    public record PeerAddress(int nodeId, String host, int port) {
        public String baseUrl() {
            return "http://" + host + ":" + port;
        }
    }

    @Override
    public String toString() {
        return "NodeConfig{" + "id=" + nodeId + ", port=" + port + ", dataDir='" + dataDir + '\'' + 
        ", peers=" + peers.values() + ", clusterSize=" + clusterSize() + ", majority=" + majority()
        + ", snapshotThreshold=" + snapshotThreshold + ", allowOrigin=" + allowOrigin + '}';
    }
}