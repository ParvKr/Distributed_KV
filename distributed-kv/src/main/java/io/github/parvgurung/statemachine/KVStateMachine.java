package io.github.parvgurung.statemachine;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class KVStateMachine {
    
    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public synchronized void apply(Command command, long logIndex) {
        if (logIndex != lastAppliedIndex.get() + 1) {
            throw new IllegalStateException("Log index out of order. Expected: " + (lastAppliedIndex.get() + 1) + ", but got: " + logIndex);
        }

        switch (command.type()) {
            case SET -> data.put(command.key(), command.value());
            case DELETE -> data.remove(command.key());
            default -> throw new IllegalArgumentException("Unknown command type: " + command.type());
        }

        lastAppliedIndex.set(logIndex);
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