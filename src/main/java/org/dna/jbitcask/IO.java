package org.dna.jbitcask;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class IO {

    static class BCFileLock {

        private final FileChannel channel;
        private final Path filename;
        private final boolean writelock;

        public BCFileLock(FileChannel channel, Path filename, boolean writelock) {
            this.channel = channel;
            this.filename = filename;
            this.writelock = writelock;
        }

        public boolean isWritelock() {
            return writelock;
        }

        public Path getFilename() {
            return filename;
        }

        public FileChannel getFileChannel() {
            return channel;
        }
    }

    static void lockRelease(BCFileLock lock) {
        if (lock.channel != null) {
            // If this is a write lock, we need to delete the file as part of cleanup. But be
            // sure to do this BEFORE letting go of the file handle so as to ensure consistency
            // with other readers.
            if (lock.writelock) {
                try {
                    Files.delete(lock.filename);
                } catch (IOException e) {
                    // TODO: Come up with some way to complain/error log if this unlink failed for some
                    // reason!!
                    e.printStackTrace();
                }
            }
        }
    }

    static BCFileLock lockAcquire(Path filename, boolean writelock) throws IOException {
        // Setup the flags for the lock file
        Set<OpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.READ);
        if (writelock) {
            // Use O_SYNC (in addition to other flags) to ensure that when we write
            // data to the lock file it is immediately (or nearly) available to any
            // other reading processes
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(StandardOpenOption.WRITE);
            options.add(StandardOpenOption.SYNC);
        }
        // Try to open the lock file
        final Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("rw-------");
        final FileAttribute<Set<PosixFilePermission>> permsAttrs = PosixFilePermissions.asFileAttribute(filePerms);
        FileChannel channel = FileChannel.open(filename, options, permsAttrs);
        return new BCFileLock(channel, filename, writelock);
    }

    public static String lockReaddata(BCFileLock lock) throws IOException {
        final long size = lock.channel.size();
        // TODO check size <= MAX_INT
        final ByteBuffer buffer = ByteBuffer.allocate((int) size);
        // TODO check we have read all data,
        // Read the whole file into our binary
        lock.channel.read(buffer);
        buffer.flip();
        byte[] arr = new byte[buffer.remaining()];
        buffer.get(arr);
        return new String(arr);
    }

    public static void lockWritedata(BCFileLock lock, String content) throws IOException {
        if (!lock.isWritelock()) {
            // Tried to write data to a read lock
            throw new IOException("LOCK_NOT_WRITABLE");
        }

        // Truncate the file first, to ensure that the lock file only contains what
        // we're about to write
        lock.channel.truncate(0L);

        // Write the new blob of data to the lock file. Note that we use O_SYNC to
        // ensure that the data is available ASAP to reading processes.
        final byte[] rawString = content.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer bb = ByteBuffer.allocate(rawString.length);

        lock.channel.write(bb.put(rawString).flip());
    }

    static FileChannel fileOpen(String filename, List<FileOperations.OpenMode> opts) throws IOException {
        Set<OpenOption> options = new HashSet<>();
        if (opts.contains(FileOperations.OpenMode.READONLY)) {
            options.add(StandardOpenOption.READ);
        }
        return FileChannel.open(Path.of(filename), options);
    }

}
