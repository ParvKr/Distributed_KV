package io.github.parvgurung.node;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ElectionTimerTest {
    @Test
    void hasElapsedOnlyAfterTheTimeoutRunsOut() throws Exception {
        CountDownLatch fired = new CountDownLatch(1);
        ElectionTimer timer = new ElectionTimer(fired::countDown, 50, 60);
        try {
            timer.start();
            assertFalse(timer.hasElapsed());
            assertTrue(fired.await(2, TimeUnit.SECONDS));
            assertTrue(timer.hasElapsed());
        } finally {
            timer.shutdown();
        }
    }

    @Test
    void resetMakesAnInFlightTimeoutStale() throws Exception {
        CountDownLatch fired = new CountDownLatch(1);
        ElectionTimer timer = new ElectionTimer(fired::countDown, 50, 60);
        try {
            timer.start();
            assertTrue(fired.await(2, TimeUnit.SECONDS));
            timer.reset(); // e.g. a heartbeat arrived while the timeout task was waiting for the node lock
            assertFalse(timer.hasElapsed(), "a reset must invalidate a timeout that already started running");
        } finally {
            timer.shutdown();
        }
    }

    @Test
    void cancelledTimerNeverReportsElapsed() throws Exception {
        ElectionTimer timer = new ElectionTimer(() -> { }, 50, 60);
        try {
            timer.start();
            timer.cancel();
            Thread.sleep(150);
            assertFalse(timer.hasElapsed());
        } finally {
            timer.shutdown();
        }
    }

    @Test
    void resetAfterShutdownIsHarmless() {
        ElectionTimer timer = new ElectionTimer(() -> { }, 50, 60);
        timer.shutdown();
        assertDoesNotThrow(timer::reset);
    }

    @Test
    void timerKeepsFiringAfterTheHandlerThrows() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch secondCall = new CountDownLatch(1);
        ElectionTimer timer = new ElectionTimer(() -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("boom");
            secondCall.countDown();
        }, 50, 60);
        try {
            timer.start();
            assertTrue(secondCall.await(3, TimeUnit.SECONDS), "timer was never re-armed after the handler threw");
        } finally {
            timer.shutdown();
        }
    }
}
