package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.PersistentLog;
import io.github.parvgurung.rpc.*;
import io.github.parvgurung.state.RaftPersistentState;
import io.github.parvgurung.statemachine.KVStateMachine;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class RaftNode implements RaftRpcHandler{
    private static final int ELECTION_TIMEOUT_MIN_MS = 800;
    private static final int ELECTION_TIMEOUT_MAX_MS = 1500;
    private static final int HEARTBEAT_INTERVAL_MS = 100;

    private final NodeConfig config;
    private final RaftPersistentState persistentState;
    private final PersistentLog log;
    private final KVStateMachine stateMachine;
    private final RpcClient rpcClient;

    private final ElectionTimer electionTimer;
    private final ExecutorService rpcExecutor;
    private final ScheduledExecutorService heartbeatScheduler;

    private RpcServer rpcServer;
    private ScheduledFuture<?> heartbeatTask;

    private NodeRole role = NodeRole.FOLLOWER;
    private Long currentLeaderId = null;
    private long commitIndex = 0;

    public RaftNode(NodeConfig config) {
        this.config = config;
        this.persistentState = new RaftPersistentState(config.dataDir.toString());
        this.log = new PersistentLog(config.dataDir.toString());
        this.stateMachine = new KVStateMachine();
        this.rpcClient = new RpcClient();

        this.electionTimer = new ElectionTimer(this::onElectionTimeout, ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
        this.rpcExecutor = Executors.newCachedThreadPool(daemonFactory("raft-rpc"));
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory("raft-heartbeat"));
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger count = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + count.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    // =================================================================
    // Lifecycle
    // =================================================================
    public synchronized void start() throws IOException {
        persistentState.init();
        log.init();
        rpcServer = new RpcServer(config.port, this);
        rpcServer.start();
        electionTimer.start();
    }

    public synchronized void shutdown() throws IOException {
        electionTimer.shutdown();
        cancelHeartbeat();
        rpcExecutor.shutdownNow();
        heartbeatScheduler.shutdownNow();
        if (rpcServer != null) rpcServer.stop();
            log.close();
    }

    // =================================================================
    // Election timeout -> becoming a candidate -> requesting votes
    // =================================================================
    private void onElectionTimeout() {
        synchronized (this) {
            if (role == NodeRole.LEADER) {
            return;
            }
        }
        startElection();
    }

    private void startElection() {
        final long electionTerm;
        final long lastLogIndex;
        final long lastLogTerm;
        final int myId = config.nodeId;
        synchronized (this) {
            if (role == NodeRole.LEADER) return;
            role = NodeRole.CANDIDATE;
            persistentState.setCurrentTerm(persistentState.getCurrentTerm() + 1);
            persistentState.setVotedFor(myId);
            currentLeaderId = null;

            electionTerm = persistentState.getCurrentTerm();
            lastLogIndex = log.lastIndex();
            lastLogTerm = log.lastTerm();
        }
        electionTimer.reset();

        if (config.peers.isEmpty()) {
            synchronized (this) {
                if (role == NodeRole.CANDIDATE && persistentState.getCurrentTerm() == electionTerm)
                    becomeLeader();
            }
            return;
        }
        RequestVoteRequest request = new RequestVoteRequest(electionTerm, myId, lastLogIndex, lastLogTerm);
        AtomicInteger votesGranted = new AtomicInteger(1);
        for (NodeConfig.PeerAddress peer : config.peers.values()) {
            rpcExecutor.submit(() -> requestVoteFromPeer(peer, request, electionTerm, votesGranted));
        }
    }

    private void requestVoteFromPeer(NodeConfig.PeerAddress peer, RequestVoteRequest request, long electionTerm, AtomicInteger votesGranted) {
        Optional<RequestVoteResponse> responseOpt = rpcClient.sendRequestVote(peer.baseUrl(), request);
        if (responseOpt.isEmpty()) {
            return;
        }
        RequestVoteResponse response = responseOpt.get();
        synchronized (this) {
            if (response.term() > persistentState.getCurrentTerm()) {
                becomeFollower(response.term());
                currentLeaderId = null;
                return;
            }
            if (role != NodeRole.CANDIDATE || persistentState.getCurrentTerm() != electionTerm) {
                return;
            }
            if (response.voteGranted()) {
                int total = votesGranted.incrementAndGet();
                if (total >= config.majority()) {
                    becomeLeader();
                }
            }
        }
    }
    // =================================================================
    // Role transitions -- callers must hold the lock (synchronized(this))
    // =================================================================
    private void becomeLeader() {
        role = NodeRole.LEADER;
        currentLeaderId = (long) config.nodeId;
        electionTimer.cancel();
        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    private void becomeFollower(long term) {
        if (role == NodeRole.LEADER) {
            cancelHeartbeat();
        }
        role = NodeRole.FOLLOWER;
        persistentState.setCurrentTerm(term);
        electionTimer.reset();
    }

    private void cancelHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    // =================================================================
    // Heartbeats (leader -> followers, empty AppendEntries)
    // =================================================================

    private void sendHeartbeats() {
        final long term;
        final int myId = config.nodeId;
        final long commit;
        synchronized (this) {
            if (role != NodeRole.LEADER) return;
            term = persistentState.getCurrentTerm();
            commit = commitIndex;
        }
        long prevLogIndex = log.lastIndex();
        long prevLogTerm = log.lastTerm();
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(term, myId, prevLogIndex, prevLogTerm, List.of(), commit);
        for (NodeConfig.PeerAddress peer : config.peers.values()) {
            rpcExecutor.submit(() -> {
                Optional<AppendEntriesResponse> responseOpt = rpcClient.sendAppendEntries(peer.baseUrl(), heartbeat);
                responseOpt.ifPresent(response -> {
                    synchronized (this) {
                        if (response.term() > persistentState.getCurrentTerm()) {
                            becomeFollower(response.term());
                            currentLeaderId = null;
                        }
                    }
                });
            });
        }
    }

    // =================================================================
    // RaftRpcHandler implementation -- called by RpcServer on incoming requests
    // =================================================================

    @Override
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        long currentTerm = persistentState.getCurrentTerm();
        if (request.term() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term());
            currentLeaderId = null;
        }
        currentTerm = persistentState.getCurrentTerm();
        Integer votedFor = persistentState.votedFor();
        int candidateId = Math.toIntExact(request.candidateId());
        boolean logIsUpToDate = log.isCandidateLogUpToDate(request.lastLogIndex(), request.lastLogTerm());
        boolean grant = (votedFor == null || votedFor.intValue() == candidateId) && logIsUpToDate;
        if (grant) {
            persistentState.setVotedFor(candidateId);
            electionTimer.reset();
        }
        return new RequestVoteResponse(currentTerm, grant);
    }

    @Override
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        long currentTerm = persistentState.getCurrentTerm();
        if (request.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, 0, 0);
        }
        if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
            becomeFollower(request.term());
        }
        currentLeaderId = request.leaderId();
        electionTimer.reset();
        return new AppendEntriesResponse(persistentState.getCurrentTerm(), true, 0, 0);
    }

    @Override
    public synchronized ClientResponse handleClientGet(String key) {
        if (role != NodeRole.LEADER) {
            return ClientResponse.notLeader(leaderHint());
        }
        return ClientResponse.OK(stateMachine.get(key).orElse(null));
    }

    @Override
    public synchronized ClientResponse handleClientSet(String key, String value) {
        if (role != NodeRole.LEADER) {
            return ClientResponse.notLeader(leaderHint());
        }
        return ClientResponse.error("Write path requires log replication, implemented in Part 3.");
    }

    @Override
    public synchronized ClientResponse handleClientDelete(String key) {
        if (role != NodeRole.LEADER) {
            return ClientResponse.notLeader(leaderHint());
        }
        return ClientResponse.error("Write path requires log replication, implemented in Part 3.");
    }

    private String leaderHint() {
        if (currentLeaderId == null) return null;
        Integer leaderInt = currentLeaderId == null ? null : currentLeaderId.intValue();
        NodeConfig.PeerAddress peer = config.peers.get(leaderInt);
        return peer == null ? null : peer.host() + ":" + peer.port();
    }

    @Override
    public synchronized String statusSummary() {
        return String.format("node=%d role=%s term=%d leader=%s logLastIndex=%d logLastTerm=%d commitIndex=%d %s", 
        config.nodeId, role, persistentState.getCurrentTerm(),
        currentLeaderId == null ? "unknown" : currentLeaderId.toString(),
        log.lastIndex(), log.lastTerm(), commitIndex, stateMachine
        );
    }

    public synchronized NodeRole role() {
        return role;
    }

    public synchronized long currentTerm() {
        return persistentState.getCurrentTerm();
    }

    public synchronized Long currentLeaderId() {
        return currentLeaderId;
    }

    public int nodeId() {
        return config.nodeId;
    }
}