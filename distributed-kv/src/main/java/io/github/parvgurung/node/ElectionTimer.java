package io.github.parvgurung.node;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class ElectionTimer {
    private final Runnable onTimeout;
    private final int minMs;
    private final int maxMs;
    private final Random random = new Random();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r->{
        Thread t = new Thread(r, "election-timer");
        t.setDaemon(true);
        return t;
    });

    private volatile ScheduledFuture<?> pendingFire;

    public ElectionTimer(Runnable onTimeout, int minMs, int maxMs) {
        if (minMs >= maxMs)
            throw new IllegalArgumentException("minMs must be < maxMs, got " + minMs + " >= " + maxMs);
            this.onTimeout = onTimeout;
            this.minMs = minMs;
            this.maxMs = maxMs;
    }

    public synchronized void start() {
        scheduleNext();
    }

    public synchronized void reset() {
        cancelPending();
        scheduleNext();
    }

    public synchronized void cancel() {
        cancelPending();
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    private void cancelPending() {
        if (pendingFire != null)
            pendingFire.cancel(false);
    }

    private void scheduleNext() {
        int delay = minMs + random.nextInt(maxMs - minMs + 1);
        pendingFire = scheduler.schedule(onTimeout, delay, TimeUnit.MILLISECONDS);
    }
}