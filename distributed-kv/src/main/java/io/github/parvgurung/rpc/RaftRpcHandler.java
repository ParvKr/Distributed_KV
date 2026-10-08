package io.github.parvgurung.rpc;

public interface RaftRpcHandler {
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request);
    ClientResponse handleClientGet(String key);
    /** {@code clientId} may be null (and {@code seq} 0) for a write that does not need retry protection. */
    ClientResponse handleClientSet(String key, String value, String clientId, long seq);
    ClientResponse handleClientDelete(String key, String clientId, long seq);
    String statusSummary();
    /** The same information as {@link #statusSummary()}, as a JSON object. */
    String statusJson();
}
