package io.github.parvgurung.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RaftPersistentStateAtomicTest {
    @TempDir
    Path tempDir;

    @Test
    void termAndVoteArePersistedTogether() throws IOException {
        RaftPersistentState state = new RaftPersistentState(tempDir.toString());
        state.init();
        state.setTermAndVote(5, 2);

        RaftPersistentState reloaded = new RaftPersistentState(tempDir.toString());
        reloaded.init();
        assertEquals(5, reloaded.getCurrentTerm());
        assertEquals(2, reloaded.votedFor());
    }

    @Test
    void termAndVoteCannotMoveBackward() throws IOException {
        RaftPersistentState state = new RaftPersistentState(tempDir.toString());
        state.init();
        state.setCurrentTerm(4);
        assertThrows(IllegalArgumentException.class, () -> state.setTermAndVote(3, 1));
        assertEquals(4, state.getCurrentTerm());
    }

    @Test
    void writesLeaveNoTempFileBehind() throws IOException {
        RaftPersistentState state = new RaftPersistentState(tempDir.toString());
        state.init();
        state.setCurrentTerm(2);
        state.setVotedFor(1);
        assertFalse(Files.exists(tempDir.resolve("raft.state.tmp")));
    }
}
