package io.github.parvgurung.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public final class FileUtil {
    private FileUtil() {}

    /**
     * Replaces {@code target} with {@code content} so that a crash leaves either the old or the new
     * file, never a partial one: write a sibling temp file, fsync it, atomically rename it over the
     * target, then fsync the directory.
     */
    public static void writeAtomically(Path target, byte[] content) throws IOException {
        Path dir = target.toAbsolutePath().getParent();
        Path tmp = dir.resolve(target.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        fsyncDirectory(dir);
    }

    // Best effort: some platforms cannot open a directory for fsync.
    private static void fsyncDirectory(Path dir) {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
        }
    }
}
