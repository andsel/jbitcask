package org.dna.jbitcask;

import org.dna.jbitcask.FunctionResult.Atom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LockOperations {

    enum DeleteStaleLockResult { OK, NOT_STALE;}

    enum LockType {MERGE, WRITE, CREATE}

    enum AcquireResult {LOCKED;}
    /**
     * Release a previously acquired write/merge lock.
     * */
    static void release(IO.BCFileLock lock) throws IOException {
        // If this is a write lock, we need to delete the file as part of cleanup. But be
        // sure to do this BEFORE letting go of the file handle so as to ensure consistency
        // with other readers.
        if (lock.isWritelock()) {
            // TODO: Come up with some way to complain/error log if this unlink failed for some
            // reason!!
            lock.getFilename().toFile().delete();
        }
        lock.getFileChannel().close();
    }

    static class AlreadyLockedException extends Exception {

    }

    /**
     * Attempt to lock the specified directory with a specific type of lock (merge or write).
     * */
    static IO.BCFileLock acquire(LockType type, String dirname) throws AlreadyLockedException, IOException {
        final Path lockFilename = lockFilename(type, dirname);
        try {
            final IO.BCFileLock bcFileLock = IO.lockAcquire(lockFilename, true);
            // Successfully acquired our lock. Update the file with our PID.
            IO.lockWritedata(bcFileLock, ProcessHandle.current().pid() + " \n");
            return bcFileLock;
        } catch (FileAlreadyExistsException eex) {
            // Lock file already exists, but may be stale. Delete it if it's stale
            // and try to acquire again
            final DeleteStaleLockResult deleteResult = deleteStaleLock(lockFilename);
            return switch (deleteResult) {
                case OK -> acquire(type, dirname);
                case NOT_STALE -> throw new AlreadyLockedException();
            };
        }
    }

    static DeleteStaleLockResult deleteStaleLock(LockType type, String dirname) {
        return deleteStaleLock(lockFilename(type, dirname));
    }

    static DeleteStaleLockResult deleteStaleLock(Path filename) {
        // Open the lock for read-only access. We do this to avoid race-conditions
        // with other O/S processes that are attempting the same task. Opening a
        // fd and holding it open until AFTER the unlink ensures that the file we
        // initially read is the same one we are deleting.
        try {
            final IO.BCFileLock lock = IO.lockAcquire(filename, false);
            try {
                final PidAndFilename pid = readLockData(lock);
                if (osPidExists(Long.parseLong(pid.osPid))) {
                    // The lock IS NOT stale, so we can't delete it.
                    return DeleteStaleLockResult.NOT_STALE;
                } else {
                    // The lock IS stale; delete the file.
                    final boolean deleted = filename.toFile().delete();
                    if (!deleted) {
                        System.err.println("Can't delete stale lock file: " + filename);
                    }
                    return DeleteStaleLockResult.OK;
                }
            } catch (BitCaskError ex) {
                System.err.printf("Failed to read lock data from %s: %s%n", filename, ex);
                return DeleteStaleLockResult.NOT_STALE;
            } finally {
                IO.lockRelease(lock);
            }
        } catch (FileNotFoundException fnfex) {
            // Failed to open the lock for reading, but only because it doesn't exist
            // any longer. Treat this as a successful delete; the lock file existed
            // when we started
            return DeleteStaleLockResult.OK;
        } catch (IOException ex) {
            //Failed to open the lock for reading due to other errors.
            System.err.printf("Failed to open lock file %s %s %n", filename, ex);
            return DeleteStaleLockResult.NOT_STALE;
        }
    }

    private static boolean osPidExists(long pid) {
        Optional<ProcessHandle> optionalProcessHandle = ProcessHandle.of(pid);
        if (optionalProcessHandle.isEmpty()) {
            return false;
        }
        return optionalProcessHandle.get().isAlive();
    }

    private static Path lockFilename(LockType type, String dirname) {
        return Path.of(dirname, String.join("", "bitcask.", type.toString().toLowerCase(), ".lock"));
    }

    static final class PidAndFilename {

        private final String osPid;
        private final String lockedFilename;

        public PidAndFilename(String osPid) {
            this(osPid, null);
        }

        public PidAndFilename(String osPid, String lockedFilename) {
            this.osPid = osPid;
            this.lockedFilename = lockedFilename;
        }
    }

    private static PidAndFilename readLockData(IO.BCFileLock lock) throws IOException, BitCaskError {
        final String content = IO.lockReaddata(lock);
        Pattern p = Pattern.compile("([0-9]+) (.*)\n");
        Matcher m = p.matcher(content);
        if (!m.matches()) {
            throw new BitCaskError("Lock file contains invalid data: [" + content + "]");
        }

        final String osPid = m.group(0);
        if (m.groupCount() != 1) {
            final String lockedFilename = m.group(1);
            return new PidAndFilename(osPid, lockedFilename);
        }
        return new PidAndFilename(osPid);
    }

    /**
     * Read the active filename stored in a given lockfile.
     * */
    static String readActivefile(LockType type, String dirname) {
        final Path lockFilename = lockFilename(type, dirname);
        try {
            final IO.BCFileLock bcFileLock = IO.lockAcquire(lockFilename, false);
            try {
                final PidAndFilename pidAndFilename = readLockData(bcFileLock);
                return pidAndFilename.lockedFilename;
            } catch (IOException | BitCaskError ex) {
                return null;
            }
        } catch (IOException ex) {
            return null;
        }
    }

    /**
     * Write a new active filename to an open lockfile.
     * */
    static void writeActivefile(IO.BCFileLock lock, String activeFilename) throws IOException {
        final String content = ProcessHandle.current().pid() + " " + activeFilename + "\n";
        IO.lockWritedata(lock, content);
    }
}
