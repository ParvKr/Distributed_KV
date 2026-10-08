package io.github.parvgurung.statemachine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NoopCommandTest {
    @Test
    void noopRoundTripsThroughSerialization() {
        Command c = Command.deserialize(Command.noop().serialize());
        assertEquals(CommandType.NOOP, c.type());
    }

    @Test
    void noopAdvancesAppliedIndexWithoutChangingData() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("a", "1"), 1);
        kv.apply(Command.noop(), 2);
        assertEquals(2, kv.getLastAppliedIndex());
        assertEquals(1, kv.size());
    }

    @Test
    void setWithoutValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Command.set("a", null));
    }
}
