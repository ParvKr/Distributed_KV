package io.github.parvgurung.node;

import io.github.parvgurung.rpc.*;

import java.util.Optional;
import java.util.Set;

/** An RpcClient that silently drops traffic to chosen nodes, to simulate a network partition. */
class PartitionableRpcClient extends RpcClient {
    private volatile Set<String> blocked = Set.of();

    void block(String baseUrl) {
        Set<String> next = new java.util.HashSet<>(blocked);
        next.add(baseUrl);
        blocked = Set.copyOf(next);
    }

    void unblockAll() {
        blocked = Set.of();
    }

    /** Replaces the whole blocked set in one step. */
    void setBlocked(Set<String> baseUrls) {
        blocked = Set.copyOf(baseUrls);
    }

    @Override
    public Optional<RequestVoteResponse> sendRequestVote(String baseUrl, RequestVoteRequest request) {
        return blocked.contains(baseUrl) ? Optional.empty() : super.sendRequestVote(baseUrl, request);
    }

    @Override
    public Optional<AppendEntriesResponse> sendAppendEntries(String baseUrl, AppendEntriesRequest request) {
        return blocked.contains(baseUrl) ? Optional.empty() : super.sendAppendEntries(baseUrl, request);
    }

    @Override
    public Optional<InstallSnapshotResponse> sendInstallSnapshot(String baseUrl, InstallSnapshotRequest request) {
        return blocked.contains(baseUrl) ? Optional.empty() : super.sendInstallSnapshot(baseUrl, request);
    }
}
