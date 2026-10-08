package io.github.parvgurung.node;

import io.github.parvgurung.config.NodeConfig;
import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.log.PersistentLog;
import io.github.parvgurung.rpc.*;
import io.github.parvgurung.snapshot.Snapshot;
import io.github.parvgurung.snapshot.SnapshotStore;
import io.github.parvgurung.state.RaftPersistentState;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.statemachine.KVStateMachine;
import io.github.parvgurung.util.Json;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class RaftNode implements RaftRpcHandler{
    private static final int ELECTION_TIMEOUT_MIN_MS = 800;
    private static final int ELECTION_TIMEOUT_MAX_MS = 1500;
    private static final int HEARTBEAT_INTERVAL_MS = 100;
    private static final long CLIENT_WRITE_TIMEOUT_MS = 2000;
    private static final long LEADERSHIP_CONFIRM_TIMEOUT_MS = 1000;

    private final NodeConfig config;
    private final RaftPersistentState persistentState;
    private final PersistentLog log;
    private final KVStateMachine stateMachine;
    private final SnapshotStore snapshotStore;
    private final RpcClient rpcClient;

    private final ElectionTimer electionTimer;
    private final ExecutorService rpcExecutor;
    private final ScheduledExecutorService heartbeatScheduler;
    private final ExecutorService snapshotExecutor;
    private final AtomicBoolean snapshotInFlight = new AtomicBoolean(false);
    // Followers a snapshot is currently being sent to; heartbeats must not queue duplicates of a big transfer.
    private final Set<Integer> snapshotSendsInFlight = ConcurrentHashMap.newKeySet();
    // Guards the snapshot file, and savedSnapshotIndex which is the index of what is currently in it.
    // Lock order: the node's monitor first, then this one, never the other way round.
    private final Object snapshotFileLock = new Object();
    private long savedSnapshotIndex = 0;

    private RpcServer rpcServer;
    private ScheduledFuture<?> heartbeatTask;

    private NodeRole role = NodeRole.FOLLOWER;
    private Long currentLeaderId = null;
    private long commitIndex = 0;
    private long leaderNoopIndex = 0;

    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    private final Map<Long, CompletableFuture<Boolean>> pendingClientWrites = new ConcurrentHashMap<>();

    public RaftNode(NodeConfig config) {
        this.config = config;
        this.persistentState = new RaftPersistentState(config.dataDir.toString());
        this.log = new PersistentLog(config.dataDir.toString());
        this.stateMachine = new KVStateMachine();
        this.snapshotStore = new SnapshotStore(config.dataDir.toString());
        this.rpcClient = new RpcClient();

        this.electionTimer = new ElectionTimer(this::onElectionTimeout, ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
        this.rpcExecutor = Executors.newFixedThreadPool(16);
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory("raft-heartbeat"));
        this.snapshotExecutor = Executors.newSingleThreadExecutor(daemonFactory("raft-snapshot"));
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
        Optional<Snapshot> snapshot = snapshotStore.load();
        if (snapshot.isPresent()) {
            Snapshot snap = snapshot.get();
            log.init(snap.lastIncludedIndex(), snap.lastIncludedTerm());
            stateMachine.restore(stateOf(snap));
            commitIndex = snap.lastIncludedIndex();
            savedSnapshotIndex = snap.lastIncludedIndex();
        } else {
            log.init();
        }
        rpcServer = new RpcServer(config.port, this);
        rpcServer.start();
        electionTimer.start();
    }

    public synchronized void shutdown() throws IOException {
        electionTimer.shutdown();
        cancelHeartbeat();
        rpcExecutor.shutdownNow();
        heartbeatScheduler.shutdownNow();
        snapshotExecutor.shutdownNow();
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
            // The timeout may be stale: a vote or heartbeat can have reset the timer while this task
            // waited for the lock. Starting an election then would needlessly depose a healthy leader.
            if (!electionTimer.hasElapsed()) return;
            role = NodeRole.CANDIDATE;
            persistentState.setTermAndVote(persistentState.getCurrentTerm() + 1, myId);
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
        final long term = persistentState.getCurrentTerm();
        final long noopIndex = log.lastIndex() + 1;
        try {
            // A leader can only commit entries from its own term (Raft section 5.4.2). Appending a no-op
            // gets earlier-term entries committed, and tells us when our state machine is current for reads.
            log.append(new LogEntry(noopIndex, term, Command.noop()));
        } catch (IOException e) {
            System.err.println("Node " + config.nodeId + " could not append leader no-op, staying follower: " + e.getMessage());
            becomeFollower(term);
            return;
        }
        role = NodeRole.LEADER;
        currentLeaderId = (long) config.nodeId;
        leaderNoopIndex = noopIndex;
        electionTimer.cancel();

        nextIndex.clear();
        matchIndex.clear();
        for (Integer peerId : config.peers.keySet()) {
            nextIndex.put(peerId, noopIndex);
            matchIndex.put(peerId, 0L);
        }
        tryAdvanceCommitIndex(); // a single-node cluster commits on its own
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
            if (ni <= log.snapshotIndex()) {
                // The entries this follower needs were compacted away; only a snapshot can catch it up.
                if (snapshotSendsInFlight.add(peerId)) {
                    rpcExecutor.submit(() -> {
                        try {
                            sendSnapshotToPeer(peerId, peerAddress, term);
                        } finally {
                            snapshotSendsInFlight.remove(peerId);
                        }
                    });
                }
                return;
            }
            prevLogIndex = ni - 1;
            prevLogTerm = log.termAt(prevLogIndex).orElse(0L);
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
                // Replies can arrive out of order; never let a stale one move matchIndex backwards.
                long newMatch = Math.max(matchIndex.getOrDefault(peerId, 0L), prevLogIndex + entries.size());
                matchIndex.put(peerId, newMatch);
                nextIndex.put(peerId, newMatch + 1);
                tryAdvanceCommitIndex();
            } else if (response.conflictIndex() > 0) {
                // A real log mismatch. Always step back at least one entry so the retry makes progress.
                long newNext = Math.max(1, Math.min(resolveConflict(response, myLastIndexAtSend), prevLogIndex));
                nextIndex.put(peerId, newNext);
                rpcExecutor.submit(() -> replicateToPeer(peerId, peerAddress));
            }
            // else: the follower failed for a reason unrelated to the log (e.g. disk error). Retry on the
            // next heartbeat rather than hammering it.
        }
    }

    private void sendSnapshotToPeer(int peerId, NodeConfig.PeerAddress peerAddress, long term) {
        final Snapshot snapshot;
        try {
            Optional<Snapshot> loaded;
            synchronized (snapshotFileLock) {
                loaded = snapshotStore.load();
            }
            if (loaded.isEmpty()) return;
            snapshot = loaded.get();
        } catch (IOException e) {
            System.err.println("Node " + config.nodeId + " could not read its snapshot: " + e.getMessage());
            return;
        }
        Optional<InstallSnapshotResponse> responseOpt = rpcClient.sendInstallSnapshot(peerAddress.baseUrl(),
            new InstallSnapshotRequest(term, config.nodeId, snapshot));
        if (responseOpt.isEmpty()) return;
        InstallSnapshotResponse response = responseOpt.get();
        synchronized (this) {
            if (response.term() > persistentState.getCurrentTerm()) {
                becomeFollower(response.term());
                currentLeaderId = null;
                return;
            }
            if (role != NodeRole.LEADER || persistentState.getCurrentTerm() != term) {
                return;
            }
            long newMatch = Math.max(matchIndex.getOrDefault(peerId, 0L), snapshot.lastIncludedIndex());
            matchIndex.put(peerId, newMatch);
            nextIndex.put(peerId, Math.max(nextIndex.getOrDefault(peerId, 0L), newMatch + 1));
            tryAdvanceCommitIndex();
        }
    }

    private long resolveConflict(AppendEntriesResponse response, long myLastIndexAtSend) {
        if (response.conflictTerm() == 0) {
            return Math.max(1, response.conflictIndex());
        }
        for (long i = myLastIndexAtSend; i > log.snapshotIndex(); i--) {
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
        maybeStartSnapshot();
    }

    // =================================================================
    // Snapshots
    // =================================================================
    private static KVStateMachine.State stateOf(Snapshot snapshot) {
        return new KVStateMachine.State(snapshot.lastIncludedIndex(), snapshot.data(), snapshot.sessions());
    }

    // Callers hold the lock.
    private void maybeStartSnapshot() {
        if (config.snapshotThreshold <= 0) return;
        if (stateMachine.getLastAppliedIndex() - log.snapshotIndex() < config.snapshotThreshold) return;
        if (!snapshotInFlight.compareAndSet(false, true)) return;
        try {
            snapshotExecutor.submit(this::takeSnapshot);
        } catch (RejectedExecutionException e) {
            snapshotInFlight.set(false); // shutting down
        }
    }

    private void takeSnapshot() {
        try {
            final KVStateMachine.State state;
            final long term;
            synchronized (this) {
                state = stateMachine.capture();
                Optional<Long> termOfLast = log.termAt(state.lastAppliedIndex());
                if (state.lastAppliedIndex() <= log.snapshotIndex() || termOfLast.isEmpty()) return;
                term = termOfLast.get();
            }
            Snapshot snapshot = new Snapshot(state.lastAppliedIndex(), term, state.data(), state.sessions());
            // The slow part (serialising and fsyncing) happens without the node lock, so heartbeats and
            // client requests keep flowing.
            saveSnapshotIfNewer(snapshot);
            synchronized (this) {
                log.compactThrough(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            }
        } catch (IOException | RuntimeException e) {
            // An interrupt just means the node is shutting down; the half-written temp file is harmless.
            if (!(e instanceof java.nio.channels.ClosedByInterruptException))
                System.err.println("Node " + config.nodeId + " snapshot failed: " + e);
        } finally {
            snapshotInFlight.set(false);
        }
    }

    /** Never replaces the snapshot file with an older snapshot than the one it holds. */
    private void saveSnapshotIfNewer(Snapshot snapshot) throws IOException {
        synchronized (snapshotFileLock) {
            if (snapshot.lastIncludedIndex() > savedSnapshotIndex) {
                snapshotStore.save(snapshot);
                savedSnapshotIndex = snapshot.lastIncludedIndex();
            }
        }
    }

    @Override
    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        long currentTerm = persistentState.getCurrentTerm();
        if (request.term() < currentTerm) {
            return new InstallSnapshotResponse(currentTerm);
        }
        if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
            becomeFollower(request.term());
        }
        currentLeaderId = request.leaderId();
        electionTimer.reset();

        Snapshot snapshot = request.snapshot();
        if (snapshot.lastIncludedIndex() <= commitIndex) {
            // Everything in it is already committed here; nothing to install.
            return new InstallSnapshotResponse(persistentState.getCurrentTerm());
        }
        try {
            saveSnapshotIfNewer(snapshot);
            log.compactThrough(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to install snapshot", e);
        }
        stateMachine.restore(stateOf(snapshot));
        commitIndex = snapshot.lastIncludedIndex();
        applyCommitted(); // entries kept after the snapshot may already be committed
        return new InstallSnapshotResponse(persistentState.getCurrentTerm());
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
        List<LogEntry> newEntries = request.entries();
        // What this request vouches for, before any trimming below.
        final long lastNewIndex = prevLogIndex + newEntries.size();

        final long base = log.snapshotIndex();
        if (prevLogIndex < base) {
            // Our snapshot already covers the start of this request. Those entries are committed, hence
            // identical to the leader's; skip them and continue from the snapshot boundary.
            int covered = (int) Math.min(newEntries.size(), base - prevLogIndex);
            newEntries = newEntries.subList(covered, newEntries.size());
            prevLogIndex += covered;
            prevLogTerm = log.snapshotTerm();
        }
        if (prevLogIndex >= base) {
            Optional<Long> ourPrevTerm = log.termAt(prevLogIndex);
            if (ourPrevTerm.isEmpty())
                return new AppendEntriesResponse(persistentState.getCurrentTerm(), false, log.lastIndex() + 1, 0);
            if (ourPrevTerm.get() != prevLogTerm) {
                long conflictTerm = ourPrevTerm.get();
                long conflictIndex = firstIndexOfTerm(conflictTerm);
                return new AppendEntriesResponse(persistentState.getCurrentTerm(), false, conflictIndex, conflictTerm);
            }
        }

        try {
            long index = prevLogIndex;
            for (LogEntry newEntry : newEntries) {
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

        // Only entries this request verified match the leader's log may be committed; anything beyond
        // them could be a stale suffix from an older leader.
        long newCommit = Math.min(request.leaderCommit(), lastNewIndex);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        return new AppendEntriesResponse(persistentState.getCurrentTerm(), true, 0, 0);
    }

    private long firstIndexOfTerm(long term) {
        for (long i = log.snapshotIndex() + 1; i <= log.lastIndex(); i++) {
            Optional<LogEntry> entry = log.get(i);
            if (entry.isPresent() && entry.get().term() == term)
                return i;
        }
        return log.snapshotIndex() + 1;
    }

    @Override
    public ClientResponse handleClientGet(String key) {
        final long term;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return ClientResponse.notLeader(leaderHint());
            }
            if (commitIndex < leaderNoopIndex) {
                return ClientResponse.error("Leader has not committed an entry in its term yet; retry");
            }
            term = persistentState.getCurrentTerm();
        }
        // A deposed leader can still believe it leads. Only answer once a majority has confirmed our term.
        if (!confirmLeadership(term)) {
            return ClientResponse.error("Could not confirm leadership with a majority");
        }
        synchronized (this) {
            if (role != NodeRole.LEADER || persistentState.getCurrentTerm() != term) {
                return ClientResponse.notLeader(leaderHint());
            }
            return ClientResponse.OK(stateMachine.get(key).orElse(null));
        }
    }

    private boolean confirmLeadership(long term) {
        if (config.peers.isEmpty()) return true;
        AtomicInteger acks = new AtomicInteger(1);
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        // Empty heartbeat that always passes the consistency check, so any same-term reply is an ack.
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(term, config.nodeId, 0, 0, List.of(), 0);
        for (NodeConfig.PeerAddress peer : config.peers.values()) {
            try {
                rpcExecutor.submit(() -> {
                    Optional<AppendEntriesResponse> responseOpt = rpcClient.sendAppendEntries(peer.baseUrl(), heartbeat);
                    if (responseOpt.isEmpty()) return;
                    AppendEntriesResponse response = responseOpt.get();
                    synchronized (this) {
                        if (response.term() > persistentState.getCurrentTerm()) {
                            becomeFollower(response.term());
                            currentLeaderId = null;
                        }
                    }
                    if (response.term() > term) {
                        confirmed.complete(false);
                    } else if (response.term() == term && acks.incrementAndGet() >= config.majority()) {
                        confirmed.complete(true);
                    }
                });
            } catch (RejectedExecutionException e) {
                return false; // shutting down
            }
        }
        try {
            return confirmed.get(LEADERSHIP_CONFIRM_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    @Override
    public ClientResponse handleClientSet(String key, String value, String clientId, long seq) {
        return handleClientWrite(Command.set(key, value, clientId, seq), value);
    }

    @Override
    public ClientResponse handleClientDelete(String key, String clientId, long seq) {
        return handleClientWrite(Command.delete(key, clientId, seq), null);
    }

    private ClientResponse handleClientWrite(Command command, String responseValueOnSuccess) {
        final long index;
        final CompletableFuture<Boolean> future;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return ClientResponse.notLeader(leaderHint());
            }
            if (stateMachine.isDuplicate(command.clientId(), command.seq())) {
                return ClientResponse.OK(responseValueOnSuccess); // a retry of a write that already committed
            }
            try {
                index = log.lastIndex() + 1;
                log.append(new LogEntry(index, persistentState.getCurrentTerm(), command));
            } catch (IOException e) {
                return ClientResponse.error("Failed to persist write: " + e.getMessage());
            }
            future = new CompletableFuture<>();
            pendingClientWrites.put(index, future);
            tryAdvanceCommitIndex(); // a single-node cluster commits on its own
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
        Integer leaderInt = currentLeaderId.intValue();
        NodeConfig.PeerAddress peer = config.peers.get(leaderInt);
        return peer == null ? null : peer.host() + ":" + peer.port();
    }

    @Override
    public synchronized String statusSummary() {
        return String.format("node=%d role=%s term=%d leader=%s logLastIndex=%d logLastTerm=%d commitIndex=%d snapshotIndex=%d %s", 
            config.nodeId, role, persistentState.getCurrentTerm(),
            currentLeaderId == null ? "unknown" : currentLeaderId.toString(),
            log.lastIndex(), log.lastTerm(), commitIndex, log.snapshotIndex(), stateMachine
        );
    }

    @Override
    public synchronized String statusJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("nodeId", config.nodeId);
        obj.put("role", role.name());
        obj.put("term", persistentState.getCurrentTerm());
        obj.put("leaderId", currentLeaderId);
        obj.put("lastLogIndex", log.lastIndex());
        obj.put("lastLogTerm", log.lastTerm());
        obj.put("commitIndex", commitIndex);
        obj.put("lastApplied", stateMachine.getLastAppliedIndex());
        obj.put("snapshotIndex", log.snapshotIndex());
        obj.put("snapshotThreshold", config.snapshotThreshold);
        obj.put("keys", stateMachine.size());
        return Json.encode(obj);
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

    public synchronized long snapshotIndex() {
        return log.snapshotIndex();
    }

    public synchronized long lastLogIndex() {
        return log.lastIndex();
    }

    public int nodeId() {
        return config.nodeId;
    }
}