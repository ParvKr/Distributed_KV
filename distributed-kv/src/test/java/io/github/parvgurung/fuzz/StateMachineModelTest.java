package io.github.parvgurung.fuzz;

import io.github.parvgurung.snapshot.Snapshot;
import io.github.parvgurung.statemachine.Command;
import io.github.parvgurung.statemachine.KVStateMachine;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** The state machine against a trivially correct model, including sessions, snapshots and restores. */
class StateMachineModelTest {
    private static final String[] CLIENTS = {"A", "B", "C"};
    private static final String[] KEYS = {"k1", "k2", "k3", "k4"};

    private static final class Model {
        final Map<String, String> data = new HashMap<>();
        final Map<String, Long> lastSeq = new HashMap<>();

        void apply(Command c) {
            if (c.clientId() != null && c.seq() <= lastSeq.getOrDefault(c.clientId(), 0L)) return;
            switch (c.type()) {
                case SET -> data.put(c.key(), c.value());
                case DELETE -> data.remove(c.key());
                case NOOP -> { }
            }
            if (c.clientId() != null) lastSeq.put(c.clientId(), c.seq());
        }
    }

    private static Command randomCommand(Random r, Map<String, Long> issued) {
        String key = KEYS[r.nextInt(KEYS.length)];
        String client = r.nextInt(4) == 0 ? null : CLIENTS[r.nextInt(CLIENTS.length)];
        long seq = 0;
        if (client != null) {
            long top = issued.getOrDefault(client, 0L);
            // mostly the next sequence number, sometimes a replay of an old one, sometimes a skip ahead
            seq = switch (r.nextInt(5)) {
                case 0 -> Math.max(1, top - r.nextInt(3));
                case 1 -> top + 2;
                default -> top + 1;
            };
            issued.merge(client, seq, Math::max);
        }
        return switch (r.nextInt(4)) {
            case 0 -> Command.delete(key, client, seq);
            case 1 -> Command.noop();
            default -> Command.set(key, "v" + r.nextInt(1000), client, seq);
        };
    }

    private static void assertSame(Model model, KVStateMachine kv, String context) {
        assertEquals(model.data.size(), kv.size(), context);
        for (String key : KEYS) {
            assertEquals(model.data.get(key), kv.get(key).orElse(null), context + " key " + key);
        }
        for (String client : CLIENTS) {
            long top = model.lastSeq.getOrDefault(client, 0L);
            if (top > 0) assertTrue(kv.isDuplicate(client, top), context + " dup@top " + client);
            assertFalse(kv.isDuplicate(client, top + 1), context + " dup@top+1 " + client);
        }
    }

    @Test
    void behavesLikeTheModelUnderRandomCommandSequences() {
        for (long seed = 1; seed <= 200; seed++) {
            Random r = new Random(seed);
            Model model = new Model();
            KVStateMachine kv = new KVStateMachine();
            Map<String, Long> issued = new HashMap<>();
            for (int index = 1; index <= 120; index++) {
                Command c = randomCommand(r, issued);
                model.apply(c);
                kv.apply(c, index);
                assertSame(model, kv, "seed " + seed + " index " + index);
            }
        }
    }

    @Test
    void restoringFromACaptureAtAnyPointGivesTheSameFutureBehaviour() {
        for (long seed = 1; seed <= 100; seed++) {
            Random r = new Random(seed);
            Map<String, Long> issued = new HashMap<>();
            KVStateMachine original = new KVStateMachine();
            KVStateMachine restored = null;
            int restoreAt = 1 + r.nextInt(60);
            Model model = new Model();
            for (int index = 1; index <= 120; index++) {
                Command c = randomCommand(r, issued);
                model.apply(c);
                original.apply(c, index);
                if (restored != null) restored.apply(c, index);
                if (index == restoreAt) {
                    // capture -> JSON snapshot -> back, exactly the path a real InstallSnapshot takes
                    KVStateMachine.State state = original.capture();
                    Snapshot snap = Snapshot.fromJson(new Snapshot(index, 1, state.data(), state.sessions()).toJson());
                    restored = new KVStateMachine();
                    restored.restore(new KVStateMachine.State(snap.lastIncludedIndex(), snap.data(), snap.sessions()));
                }
            }
            assertSame(model, original, "original seed " + seed);
            assertSame(model, restored, "restored seed " + seed);
            assertEquals(original.getLastAppliedIndex(), restored.getLastAppliedIndex());
        }
    }

    @Test
    void snapshotsOfStatesWithNastyKeysAndValuesRoundTrip() {
        for (long seed = 1; seed <= 200; seed++) {
            Random r = new Random(seed);
            Map<String, String> data = new HashMap<>();
            Map<String, Long> sessions = new HashMap<>();
            for (int i = r.nextInt(8); i > 0; i--) data.put(Gen.nonEmptyString(r, 10), Gen.string(r, 20));
            for (int i = r.nextInt(4); i > 0; i--) sessions.put(Gen.nonEmptyString(r, 6), 1L + r.nextInt(1000));
            Snapshot original = new Snapshot(1 + r.nextInt(1000), 1 + r.nextInt(20), data, sessions);
            assertEquals(original, Snapshot.fromJson(original.toJson()), "seed " + seed);
        }
    }
}
