/**
 * Immutable configuration for a Raft node.
 * Parses and validates startup arguments, exposing the node's identity,
 * networking information, storage location, and cluster peers.
 */

package io.github.parvgurung.config;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/* Expected CLI args:
* --id=1
* --port=8001
* --dataDir=./data (optional, defaults to ./data/node-<id>)
* --peers=2:localhost:8002,3:localhost:8003
*/

public final class NodeConfig {
    public final int nodeId;
    public final int port;
    public final Path dataDir;

    public final Map<Integer, PeerAddress> peers;
    
    // Constructor for NodeConfig
    public NodeConfig(int nodeId, int port, Path dataDir, Map<Integer, PeerAddress> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = dataDir;
        this.peers = Map.copyOf(peers);
    }

    // number of nodes in the cluster including this node.
    public int clusterSize() {
        return peers.size() + 1;
    }

    // number of nodes(votes) required to form a majority in the cluster and elect a leader.
    public int majority() {
        return (clusterSize() / 2) + 1;
    }

    private static int parsePositiveInt(String value, String field) {
        try {
            return Integer.parseInt(value);
        }
        catch(NumberFormatException e) {
            throw new IllegalArgumentException(field + " must be a positive integer, but got: " + value);
        }
    }

    public static NodeConfig fromArgs(String[] args) {
        Integer id = null;
        Integer port = null;
        Path dataDir = null;
        Map<Integer, PeerAddress> peers = new HashMap<>();

        for(String arg: args) {
            if(!arg.startsWith("--") || !arg.contains("="))
                continue;
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
        return new NodeConfig(id, port, dataDir, peers);
    }

    public record PeerAddress(int nodeId, String host, int port) {
        public String baseUrl() {
            return "http://" + host + ":" + port;
        }
    }

    @Override
    public String toString() {
        return "NodeConfig{" + "id=" + nodeId + ", port=" + port + ", dataDir='" + dataDir + '\'' + 
        ", peers=" + peers.values() + ", clusterSize=" + clusterSize() + ", majority=" + majority() + '}';
    }
}