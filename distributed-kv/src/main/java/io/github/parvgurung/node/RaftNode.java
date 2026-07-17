package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.log.PersistentLog;
import io.github.parvgurung.rpc.*;
import io.github.parvgurung.state.RaftPersistentState;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.statemachine.KVStateMachine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class RaftNode implements RaftRpcHandler{
    private static final int ELECTION_TIMEOUT_MIN_MS = 800;
    private static final int ELECTION_TIMEOUT_MAX_MS = 1500;
    private static final int HEARTBEAT_INTERVAL_MS = 100;
    private static final long CLIENT_WRITE_TIMEOUT_MS = 2000;

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

    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    private final Map<Long, CompletableFuture<Boolean>> pendingClientWrites = new ConcurrentHashMap<>();

    public RaftNode(NodeConfig config) {
        this.config = config;
        this.persistentState = new RaftPersistentState(config.dataDir.toString());
        this.log = new PersistentLog(config.dataDir.toString());
        this.stateMachine = new KVStateMachine();
        this.rpcClient = new RpcClient();

        this.electionTimer = new ElectionTimer(this::onElectionTimeout, ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
        this.rpcExecutor = Executors.newFixedThreadPool(16);
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

        long next = log.lastIndex() + 1;
        nextIndex.clear();
        matchIndex.clear();
        for (Integer peerId : config.peers.keySet()) {
            nextIndex.put(peerId, next);
            matchIndex.put(peerId, 0L);
        }
        cancelHeartbeat();
        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(this::replicateToAllPeers, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    private void becomeFollower(long term) {
        if (role == NodeRole.LEADER) {
            cancelHeartbeat();
            failAllPendingWrites();
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

    private void failAllPendingWrites() {
        for (CompletableFuture<Boolean> future : pendingClientWrites.values()) {
            future.complete(false);
        }
        pendingClientWrites.clear();
    }

    private void replicateToAllPeers() {
        boolean isLeader;
        synchronized (this) {
            isLeader = role == NodeRole.LEADER;
        }
        if(!isLeader) return;
        for (Map.Entry<Integer, NodeConfig.PeerAddress> entry : config.peers.entrySet()) {
            int peerId = entry.getKey();
            NodeConfig.PeerAddress peerAddress = entry.getValue();
            rpcExecutor.submit(() -> replicateToPeer(peerId, peerAddress));
        }
    }

    private void replicateToPeer(int peerId, NodeConfig.PeerAddress peerAddress) {
        final long term;
        final long prevLogIndex;
        final long prevLogTerm;
        final List<LogEntry> entries;
        final long leaderCommitSnapshot;
        final long myLastIndexAtSend;

        synchronized (this) {
            if (role != NodeRole.LEADER) return;
            term = persistentState.getCurrentTerm();
            long ni = nextIndex.getOrDefault(peerId, log.lastIndex() + 1);
            prevLogIndex = ni - 1;
            prevLogTerm = prevLogIndex == 0 ? 0 : log.get(prevLogIndex).map(LogEntry::term).orElse(0L);
            myLastIndexAtSend = log.lastIndex();
            List<LogEntry> toSend = new ArrayList<>();
            for (long i = ni; i <= myLastIndexAtSend; i++) {
                log.get(i).ifPresent(toSend::add);
            }
            entries = toSend;
            leaderCommitSnapshot = commitIndex;
        }

        AppendEntriesRequest request = new AppendEntriesRequest(term, config.nodeId, prevLogIndex, prevLogTerm, entries, leaderCommitSnapshot);
        Optional<AppendEntriesResponse> responseOpt = rpcClient.sendAppendEntries(peerAddress.baseUrl(), request);
        if (responseOpt.isEmpty()) return;
        AppendEntriesResponse response = responseOpt.get();
        synchronized (this) {
            if (response.term() > persistentState.getCurrentTerm()) {
                becomeFollower(response.term());
                currentLeaderId = null;
                return;
            }
            if (role != NodeRole.LEADER || persistentState.getCurrentTerm() != term) {
                return;
            }
            if (response.success()) {
                long newMatch = prevLogIndex + entries.size();
                matchIndex.put(peerId, newMatch);
                nextIndex.put(peerId, newMatch + 1);
                tryAdvanceCommitIndex();
            } else {
                long newNext = resolveConflict(response, myLastIndexAtSend);
                nextIndex.put(peerId, newNext);
                rpcExecutor.submit(() -> replicateToPeer(peerId, peerAddress));
            }
        }
    }

    private long resolveConflict(AppendEntriesResponse response, long myLastIndexAtSend) {
        if (response.conflictTerm() == 0) {
            return Math.max(1, response.conflictIndex());
        }
        for (long i = myLastIndexAtSend; i >= 1; i--) {
            Optional<LogEntry> entry = log.get(i);
            if (entry.isPresent() && entry.get().term() == response.conflictTerm())
                return i + 1;
        }
        return response.conflictIndex();
    }

    private void tryAdvanceCommitIndex() {
        long selfLast = log.lastIndex();
        for (long n = selfLast; n > commitIndex; n--) {
            Optional<LogEntry> entry = log.get(n);
            if (entry.isEmpty() || entry.get().term() != persistentState.getCurrentTerm())
                continue;
            int count = 1;
            for (long m : matchIndex.values()) {
                if (m >= n) count++;
            }
            if (count >= config.majority()) {
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    private void applyCommitted() {
        while (stateMachine.getLastAppliedIndex() < commitIndex) {
            long next = stateMachine.getLastAppliedIndex() + 1;
            Optional<LogEntry> entry = log.get(next);
            if (entry.isEmpty()) break;
            stateMachine.apply(entry.get().command(), next);
            CompletableFuture<Boolean> future = pendingClientWrites.remove(next);
            if (future != null) future.complete(true);
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

        long prevLogIndex = request.prevLogIndex();
        long prevLogTerm = request.prevLogTerm();
        if (prevLogIndex > 0) {
            Optional<LogEntry> prevEntry = log.get(prevLogIndex);
            if (prevEntry.isEmpty())
                return new AppendEntriesResponse(persistentState.getCurrentTerm(), false, log.lastIndex() + 1, 0);
            if (prevEntry.get().term() != prevLogTerm) {
                long conflictTerm = prevEntry.get().term();
                long conflictIndex = firstIndexOfTerm(conflictTerm);
                return new AppendEntriesResponse(persistentState.getCurrentTerm(), false, conflictIndex, conflictTerm);
            }
        }

        try {
            long index = prevLogIndex;
            for (LogEntry newEntry : request.entries()) {
                index++;
                Optional<LogEntry> existing = log.get(index);
                if (existing.isPresent()) {
                    if (existing.get().term() != newEntry.term()) {
                        log.truncateFrom(index);
                        log.append(new LogEntry(index, newEntry.term(), newEntry.command()));
                    }
                } else {
                    log.append(new LogEntry(index, newEntry.term(), newEntry.command()));
                }
            }
        } catch (IOException e) {
            return new AppendEntriesResponse(persistentState.getCurrentTerm(), false, 0, 0);
        }

        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), log.lastIndex());
            applyCommitted();
        }
        return new AppendEntriesResponse(persistentState.getCurrentTerm(), true, 0, 0);
    }

    private long firstIndexOfTerm(long term) {
        for (long i = 1; i <= log.lastIndex(); i++) {
            Optional<LogEntry> entry = log.get(i);
            if (entry.isPresent() && entry.get().term() == term)
                return i;
        }
        return 1;
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
        return handleClientWrite(Command.set(key, value), value);
    }

    @Override
    public synchronized ClientResponse handleClientDelete(String key) {
        return handleClientWrite(Command.delete(key), null);
    }

    private ClientResponse handleClientWrite(Command command, String responseValueOnSuccess) {
        final long index;
        final CompletableFuture<Boolean> future;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return ClientResponse.notLeader(leaderHint());
            }
            try {
                index = log.lastIndex() + 1;
                log.append(new LogEntry(index, persistentState.getCurrentTerm(), command));
            } catch (IOException e) {
                return ClientResponse.error("Failed to persist write: " + e.getMessage());
            }
            future = new CompletableFuture<>();
            pendingClientWrites.put(index, future);
        }
        replicateToAllPeers();
        try {
            Boolean committed = future.get(CLIENT_WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (Boolean.TRUE.equals(committed)) {
                return ClientResponse.OK(responseValueOnSuccess);
            }
            return ClientResponse.error("Write was not committed (leadership likely changed mid-write)");
        } catch (TimeoutException e) {
            return ClientResponse.error("Write timed out waiting for majority replication");
        } catch (Exception e) {
            return ClientResponse.error("Write failed: " + e.getMessage());
        } finally {
            pendingClientWrites.remove(index);
        }
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

    public synchronized Long currentLeaderId() {
        return currentLeaderId;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public int nodeId() {
        return config.nodeId;
    }
}