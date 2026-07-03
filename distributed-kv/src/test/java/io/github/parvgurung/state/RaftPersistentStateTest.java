package io.github.parvgurung.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RaftPersistentStateTest {

    @TempDir
    Path tempDir;

    @Test
    void testFreshInitialization() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        assertEquals(0, state.getCurrentTerm());
        assertNull(state.votedFor());
    }

    @Test
    void testSetCurrentTerm() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(5);

        assertEquals(5, state.getCurrentTerm());
        assertNull(state.votedFor());
    }

    @Test
    void testSetCurrentTermSameValue() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(3);
        state.setCurrentTerm(3);

        assertEquals(3, state.getCurrentTerm());
    }

    @Test
    void testCannotMoveTermBackward() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(5);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.setCurrentTerm(4)
        );
    }

    @Test
    void testVoteIsResetWhenTermAdvances() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setVotedFor(7);

        assertEquals(7, state.votedFor());

        state.setCurrentTerm(2);

        assertNull(state.votedFor());
    }

    @Test
    void testSetVotedFor() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setVotedFor(11);

        assertEquals(11, state.votedFor());
    }

    @Test
    void testClearVote() throws IOException {
        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setVotedFor(8);

        state.setVotedFor(null);

        assertNull(state.votedFor());
    }

    @Test
    void testPersistenceAcrossRestart() throws IOException {

        {
            RaftPersistentState state =
                    new RaftPersistentState(tempDir.toString());

            state.init();

            state.setCurrentTerm(9);
            state.setVotedFor(3);
        }

        {
            RaftPersistentState state =
                    new RaftPersistentState(tempDir.toString());

            state.init();

            assertEquals(9, state.getCurrentTerm());
            assertEquals(3, state.votedFor());
        }
    }

    @Test
    void testPersistenceWithNullVote() throws IOException {

        {
            RaftPersistentState state =
                    new RaftPersistentState(tempDir.toString());

            state.init();

            state.setCurrentTerm(4);
            state.setVotedFor(null);
        }

        {
            RaftPersistentState state =
                    new RaftPersistentState(tempDir.toString());

            state.init();

            assertEquals(4, state.getCurrentTerm());
            assertNull(state.votedFor());
        }
    }

    @Test
    void testMultipleTermUpdates() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        for (int i = 1; i <= 20; i++) {
            state.setCurrentTerm(i);

            assertEquals(i, state.getCurrentTerm());
            assertNull(state.votedFor());
        }
    }

    @Test
    void testVotePersistsUntilHigherTerm() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(2);
        state.setVotedFor(5);

        assertEquals(5, state.votedFor());

        state.setCurrentTerm(2);

        assertEquals(5, state.votedFor());

        state.setCurrentTerm(3);

        assertNull(state.votedFor());
    }

    @Test
    void testToStringContainsUsefulInformation() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(7);
        state.setVotedFor(2);

        String s = state.toString();

        assertTrue(s.contains("term=7"));
        assertTrue(s.contains("votedFor=2"));
    }

    @Test
    void testInitialStateFileIsCreated() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        assertTrue(
                tempDir.resolve("raft.state").toFile().exists()
        );
    }

    @Test
    void testVoteCanBeChangedWithinSameTerm() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        state.setCurrentTerm(5);

        state.setVotedFor(1);
        state.setVotedFor(2);

        assertEquals(2, state.votedFor());
    }

    @Test
    void testManyWritesRemainConsistent() throws IOException {

        RaftPersistentState state =
                new RaftPersistentState(tempDir.toString());

        state.init();

        for (int i = 1; i <= 50; i++) {
            state.setCurrentTerm(i);
            state.setVotedFor(i);
        }

        assertEquals(50, state.getCurrentTerm());
        assertEquals(50, state.votedFor());
    }
}