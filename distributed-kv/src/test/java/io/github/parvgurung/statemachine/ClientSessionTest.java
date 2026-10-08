package io.github.parvgurung.statemachine;

import io.github.parvgurung.log.LogEntry;
import io.github.parvgurung.rpc.AppendEntriesRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClientSessionTest {
    // ------------------------------------------------------------ Command

    @Test
    void sessionFieldsRoundTripThroughSerialization() {
        Command original = Command.set("k", "v", "client-1", 7);
        Command back = Command.deserialize(original.serialize());
        assertEquals(original, back);
        assertEquals("client-1", back.clientId());
        assertEquals(7, back.seq());
    }

    @Test
    void deleteSessionFieldsRoundTrip() {
        Command original = Command.delete("k", "c|1", 3); // awkward character in the ID
        assertEquals(original, Command.deserialize(original.serialize()));
    }

    @Test
    void commandsWithoutASessionKeepTheOldFormat() {
        assertEquals("SET|k|v", Command.set("k", "v").serialize());
        Command legacy = Command.deserialize("SET|k|v");
        assertNull(legacy.clientId());
        assertEquals(0, legacy.seq());
    }

    @Test
    void clientIdAndSeqMustComeTogether() {
        assertThrows(IllegalArgumentException.class, () -> Command.set("k", "v", "c", 0));
        assertThrows(IllegalArgumentException.class, () -> Command.set("k", "v", "", 1));
        assertThrows(IllegalArgumentException.class, () -> new Command(CommandType.SET, "k", "v", null, 5));
    }

    @Test
    void fourFieldLineIsStillMalformed() {
        assertThrows(IllegalArgumentException.class, () -> Command.deserialize("SET|a|b|c"));
    }

    // ------------------------------------------------------------ KVStateMachine

    @Test
    void retriedRequestIsAppliedOnlyOnce() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("k", "1", "A", 1), 1);
        kv.apply(Command.set("k", "2", "B", 1), 2);
        kv.apply(Command.set("k", "1", "A", 1), 3); // A's retry, after B has overwritten

        assertEquals("2", kv.get("k").orElseThrow());
        assertEquals(3, kv.getLastAppliedIndex(), "a duplicate still consumes its log slot");
    }

    @Test
    void olderSequenceFromTheSameClientIsIgnored() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("k", "new", "A", 2), 1);
        kv.apply(Command.set("k", "old", "A", 1), 2);
        assertEquals("new", kv.get("k").orElseThrow());
    }

    @Test
    void clientsAreTrackedIndependently() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("a", "1", "A", 1), 1);
        kv.apply(Command.set("b", "1", "B", 1), 2);
        assertEquals(2, kv.size());
    }

    @Test
    void commandsWithoutASessionAreAlwaysApplied() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.set("k", "1"), 1);
        kv.apply(Command.set("k", "2"), 2);
        assertEquals("2", kv.get("k").orElseThrow());
    }

    @Test
    void duplicateDeleteDoesNotRemoveANewerValue() {
        KVStateMachine kv = new KVStateMachine();
        kv.apply(Command.delete("k", "A", 1), 1);
        kv.apply(Command.set("k", "back", "B", 1), 2);
        kv.apply(Command.delete("k", "A", 1), 3); // retry of A's delete
        assertEquals("back", kv.get("k").orElseThrow());
    }

    // ------------------------------------------------------------ wire format

    @Test
    void appendEntriesCarriesTheSessionFields() {
        AppendEntriesRequest req = new AppendEntriesRequest(1, 2, 0, 0, List.of(
            new LogEntry(1, 1, Command.set("k", "v", "A", 4)),
            new LogEntry(2, 1, Command.set("x", "y")),
            new LogEntry(3, 1, Command.noop())), 0);

        AppendEntriesRequest back = AppendEntriesRequest.fromJson(req.toJson());

        assertEquals(req.entries(), back.entries());
    }

    @Test
    void appendEntriesFromAnOlderPeerWithoutSessionFieldsStillParses() {
        String json = "{\"term\":1,\"leaderId\":2,\"prevLogIndex\":0,\"prevLogTerm\":0,\"leaderCommit\":0,"
            + "\"entries\":[{\"index\":1,\"term\":1,\"commandType\":\"SET\",\"key\":\"k\",\"value\":\"v\"}]}";
        AppendEntriesRequest req = AppendEntriesRequest.fromJson(json);
        assertNull(req.entries().get(0).command().clientId());
    }
}
