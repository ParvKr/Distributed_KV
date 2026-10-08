package io.github.parvgurung.statemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class KVStateMachine {
    
    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);
    // Highest sequence number applied per client. A client has one write in flight at a time, so any
    // sequence number at or below this is a retry of something already applied.
    private final ConcurrentHashMap<String, Long> lastSeqByClient = new ConcurrentHashMap<>();

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    /** A consistent copy of everything the state machine holds, as of {@code lastAppliedIndex}. */
    public record State(long lastAppliedIndex, Map<String, String> data, Map<String, Long> sessions) {}

    public synchronized State capture() {
        return new State(lastAppliedIndex.get(), new HashMap<>(data), new HashMap<>(lastSeqByClient));
    }

    /** Replaces the whole state, e.g. from a snapshot. */
    public synchronized void restore(State state) {
        data.clear();
        data.putAll(state.data());
        lastSeqByClient.clear();
        lastSeqByClient.putAll(state.sessions());
        lastAppliedIndex.set(state.lastAppliedIndex());
    }

    public synchronized void apply(Command command, long logIndex) {
        if (logIndex != lastAppliedIndex.get() + 1) {
            throw new IllegalStateException("Log index out of order. Expected: " + (lastAppliedIndex.get() + 1) + ", but got: " + logIndex);
        }

        if (!isDuplicate(command.clientId(), command.seq())) {
            switch (command.type()) {
                case SET -> data.put(command.key(), command.value());
                case DELETE -> data.remove(command.key());
                case NOOP -> { }
                default -> throw new IllegalArgumentException("Unknown command type: " + command.type());
            }
            if (command.clientId() != null)
                lastSeqByClient.put(command.clientId(), command.seq());
        }

        lastAppliedIndex.set(logIndex);
    }

    /** True if this client's request {@code seq} (or a later one) has already been applied. */
    public boolean isDuplicate(String clientId, long seq) {
        return clientId != null && seq <= lastSeqByClient.getOrDefault(clientId, 0L);
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex.get();
    }

    public int size() {
        return data.size();
    }

    @Override
    public String toString() {
        return "KVStateMachine{" + "entries=" + data.size() + ", lastApplied=" + lastAppliedIndex.get() + '}';
    }
}