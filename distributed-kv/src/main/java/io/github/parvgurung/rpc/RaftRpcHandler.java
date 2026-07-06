package io.github.parvgurung.rpc;

public interface RaftRpcHandler {
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
    ClientResponse handleClientGet(String key);
    ClientResponse handleClientSet(String key, String value);
    ClientResponse handleClientDelete(String key);
    String statusSummary();
}
