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
    private long deadlineNanos;
    private boolean armed = false;   // nanoTime() may be negative, so no sentinel value can mean "never"

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

    /**
     * True only if the timer has run out and nothing has reset or cancelled it since. A timeout task that
     * was already running when reset() was called cannot be cancelled, so the owner re-checks this under
     * its own lock before acting on a timeout.
     */
    public synchronized boolean hasElapsed() {
        return armed && System.nanoTime() - deadlineNanos >= 0;
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    private void cancelPending() {
        armed = false;
        if (pendingFire != null)
            pendingFire.cancel(false);
    }

    private void fire() {
        try {
            onTimeout.run();
        } catch (RuntimeException e) {
            // An uncaught exception would silently kill the timer and the node could never elect again.
            System.err.println("Election timeout handler failed, re-arming timer: " + e);
            reset();
        }
    }

    private void scheduleNext() {
        if (scheduler.isShutdown())
            return;
        int delay = minMs + random.nextInt(maxMs - minMs + 1);
        deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        armed = true;
        pendingFire = scheduler.schedule(this::fire, delay, TimeUnit.MILLISECONDS);
    }
}