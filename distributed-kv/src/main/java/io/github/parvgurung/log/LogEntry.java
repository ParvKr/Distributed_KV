package io.github.parvgurung.log;

import io.github.parvgurung.statemachine.Command;
import java.util.Objects;

public record LogEntry(long index, long term, Command command) {
    public LogEntry {
        if (index <= 0) throw new IllegalArgumentException("Index must start from 1");
        if (term <= 0) throw new IllegalArgumentException("Term must start from 1");
        Objects.requireNonNull(command, "Command cannot be null");
    }

    public String serialize() {
        return String.join(" | ", Long.toString(index), Long.toString(term), command.serialize());
    }

    public static LogEntry deserialize(String line) {
        var parts = line.split(" \\| ", 3);
        if(parts.length != 3) {
            throw new IllegalArgumentException("Invalid serialized log entry: " + line);
        }
        long index = Long.parseLong(parts[0]);
        long term = Long.parseLong(parts[1]);
        Command command = Command.deserialize(parts[2]);
        return new LogEntry(index, term, command);
    }
}

