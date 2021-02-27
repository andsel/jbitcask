package org.dna.jbitcask;

import org.dna.jbitcask.FileOperations.FileState;
import org.dna.jbitcask.FileOperations.KeyFoldMode;
import org.dna.jbitcask.LockOperations.LockType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.stream.Collectors;

public class JBitCask {

    private static final Logger LOG = LogManager.getLogger(JBitCask.class);

    static final String TOMBSTONE_PREFIX = "bitcask_tombstone";
    static final String TOMBSTONE0_STR = TOMBSTONE_PREFIX;
    static final byte[] TOMBSTONE0 = TOMBSTONE0_STR.getBytes(StandardCharsets.UTF_8);
    static final String TOMBSTONE1_STR = TOMBSTONE_PREFIX + "1";
    static final byte[] TOMBSTONE1_BIN = TOMBSTONE1_STR.getBytes(StandardCharsets.UTF_8);
    static final String TOMBSTONE2_STR = TOMBSTONE_PREFIX + "2";
    static final byte[] TOMBSTONE2_BIN = TOMBSTONE1_STR.getBytes(StandardCharsets.UTF_8);
    static final int TOMBSTONE0_SIZE = TOMBSTONE0.length;
    // Size of tombstone + 32 bit file id
    static final int TOMBSTONE1_SIZE = TOMBSTONE1_BIN.length + 4;
    static final int TOMBSTONE2_SIZE = TOMBSTONE2_BIN.length + 4;
    // Change this to the largest size a tombstone value can have if more added.
    static final int MAX_TOMBSTONE_SIZE = TOMBSTONE2_SIZE;

    // Notice that tombstone version 1 and 2 are the same size, so not tested below
    static boolean isTombstoneSize(int s) {
        return s == TOMBSTONE0_SIZE || s == TOMBSTONE1_SIZE;
    }

    static final int OFFSETFIELD_V1 = 64;
    static final int TOMBSTONEFIELD_V2 = 1;
    static final int OFFSETFIELD_V2 = 63;
    static final int TSTAMPFIELD = 32;
    static final int KEYSIZEFIELD = 16;
    static final int TOTALSIZEFIELD = 32;
    static final int VALSIZEFIELD = 32;
    static final int CRCSIZEFIELD = 32;
    static final int HEADER_SIZE = 14; // 4 + 4 + 2 + 4 bytes
    static final long MAXKEYSIZE = 0xFFFF;
    static final long MAXVALSIZE = 0xFFFF_FFFF;
    static final long MAXOFFSET_V2 = 0x7FFF_FFFF_FFFF_FFFFL; //max 63-bit unsigned

    //for hintfile validation
    public static final int CHUNK_SIZE = 65535;
    public static final int MIN_CHUNK_SIZE = 1024;
    public static final int MAX_CHUNK_SIZE = 134217728;

    // Bitcask instance state
    private String dirname;
    private List<String> readFiles;
    private File writeFile;
    private Lock writeLock;
    private long maxFileSize;
    //TODO opts
    private Keydir keydir;
    private Function<byte[], byte[]> keyTransform;
    private byte tombstoneVersion;
    private boolean readWriteP;

    public static JBitCask open(String dirname) throws IOException, InterruptedException, TimeoutException {
        return open(dirname, new Properties());
    }

    public static JBitCask open(String dirname, Properties opts) throws IOException, InterruptedException, TimeoutException {
        final Path path = Paths.get(dirname, "bitcask");
        if (!path.toFile().exists()) {
            throw new FileNotFoundException("bitcask directory doesn't exists: " + path);
        }

        File writingFile = null;

        // If the read_write option is set, attempt to release any stale write lock.
        // Do this first to avoid unnecessary processing of files for reading.
        if ((Boolean) opts.getOrDefault("read_write", Boolean.FALSE)) {
            // If the lock file is not stale, we'll continue initializing
            // and loading anyway: if later someone tries to write
            // something, that someone will get a write_locked exception.
            LockOperations.deleteStaleLock(LockType.WRITE, dirname);
            //TODO writingFile is fresh
        } else {
            //TODO writingFile is undefined
            writingFile = null;
        }

        // Get the max file size parameter from opts
        final Long maxFileSize = getOption("max_file_size", opts);

        // Get the number of seconds we are willing to wait for the keydir init to timeout
        final Long waitMillis = getOption("open_timeout", opts);
        final long waitTime = waitMillis / 1000;

        // Set the key transform for this cask
        Function<byte[], byte[]> keyTransformer = getKeyTransform(); //opts key_transform

        //Type of tombstone to write, for testing.
        byte tombstoneVersion = getOption("tombstone_version", opts); // 0 or 2

        // Loop and wait for the keydir to come available.
        boolean readWriteP = writingFile != null;
        boolean readWriteI;
        if (readWriteP)
            readWriteI = true;
        else
            readWriteI = false;

        final Keydir keydir = initKeydir(dirname, waitTime, readWriteP, keyTransformer);
        final JBitCask bc = new JBitCask();
        bc.dirname = dirname;
        bc.readFiles = new ArrayList<>();
        bc.writeFile = writingFile; // <fd>|null|fresh
        bc.writeLock = null;
        bc.maxFileSize = maxFileSize;
        //TODO
//        bc.opts = extOpt;
        bc.keydir = keydir;
        bc.keyTransform = keyTransformer;
        bc.tombstoneVersion = tombstoneVersion;
        bc.readWriteP = readWriteI;
        return bc;
    }

    /*
     * Initialize a keydir for a given directory.
     * */
    private static Keydir initKeydir(String dirname, long waitTimeSecs, boolean readWriteMode, Function<byte[], byte[]> keyTransformer) throws IOException, InterruptedException, TimeoutException {
        // Get the named keydir for this directory. If we get it and it's already
        // marked as ready, that indicates another caller has already loaded
        // all the data from disk and we can short-circuit scanning all the files.
        final FunctionResult<FunctionResult.Atom, Object> result = Keydir.create(dirname);
        if (result.getAtom() == FunctionResult.Atom.READY) {
            // A keydir already exists, nothing more to do here. We'll lazy
            // open files as needed.
            return (Keydir) result.getResult();
        } else if (result.getAtom() == FunctionResult.Atom.NOT_READY) {
            if (result.getResult() instanceof Keydir) {
                final Keydir keydir = (Keydir) result.getResult();
                // We've just created a new named keydir, so we need to load up all
                // the data from disk. Build a list of all the bitcask data files
                // and sort it in ascending order (oldest->newest).
                //
                // We need the SortedFiles list to be stable: we might be
                // in a situation:
                // 1. Someone else starts a merge on this cask.
                // 2. Our caller opens the cask and gets to here.
                // 3. The merge races with readable_files(): creating
                //    new data files and deleting old ones.
                // 4. SortedFiles doesn't contain the list of all of the
                //    files that we need.
                try {
                    final IO.BCFileLock lock = pollForMergeLock(dirname);
                    try {
                        if (readWriteMode) {
                            // This purge will acquire the write lock
                            // prior to doing anything.
                            purgeSetuidFiles(dirname);
                        }
                        initKeydirScanKeyFiles(dirname, keydir, keyTransformer);
                    } finally {
                        IO.lockRelease(lock);
                    }
                    // Now that we loaded all the data, mark the keydir as ready
                    // so other callers can use it
                    keydir.markReady();
                    return keydir;
                } catch (IOException ex) {
                    keydir.release();
                    throw new IOException(ex);
                }

            } else {
                // error
                Thread.sleep(100);
                // avoids 'infinity'!
                if (waitTimeSecs <= 0) {
                    throw new TimeoutException("keydir init exhausted waitTimeSec");
                }
                return initKeydir(dirname, waitTimeSecs - 100, readWriteMode, keyTransformer);
            }
        } else {
            // error never happen
            throw new IllegalArgumentException("This CAN'T never happen");
        }
    }

    private static void initKeydirScanKeyFiles(String dirname, Keydir keydir, Function<byte[], byte[]> keyTransformer) {
        initKeydirScanKeyFiles(dirname, keydir, keyTransformer, Integer.MAX_VALUE);
    }

    private static void initKeydirScanKeyFiles(String dirname, Keydir keydir,
                                                 Function<byte[], byte[]> keyTransformer, int count) {
        if (count == 0) {
            // If someone launches enough parallel merge operations to
            // interfere with our attempts to scan this keydir for this many
            // times, then we are just plain unlucky.  Or QuickCheck smites us
            // from lofty Mt. Stochastic.
            throw new BitCaskError("too many iterations");
        }
        try {
            final Map<Boolean, List<Path>> files = readableAndSetuidFiles(dirname);
            final List<Path> sortedFiles = files.get(true);
            final List<Path> setuidFiles = files.get(false);
            scanKeyFiles(sortedFiles, keydir, new ArrayList<>(), true, keyTransformer);

            // There may be a setuid data file that has a larger tstamp name than
            // any non-setuid data file.  Tell the keydir about it, so that we
            // don't try to reuse that tstamp name.
            if (setuidFiles.isEmpty()) {
                return; //OK
            }
            final Optional<Long> maxSetuid = setuidFiles.stream().map(FileOperations::fileTimestamp).max(Long::compare);
            keydir.incrementFileId(maxSetuid.get());
        } catch (Exception ex) {
            System.err.println("scan_key_files:");
            initKeydirScanKeyFiles(dirname, keydir, keyTransformer, count - 1);
        }
    }

    private static List<FileState> scanKeyFiles(List<Path> files, Keydir keydir, List<FileState> acc, boolean closeFile,
                                             Function<byte[], byte[]> keyTransformer) throws IOException {
        if (files.isEmpty()) {
            return acc;
        }
        final Path filename = files.remove(0);
        final List<Path> rest = files;
        final FileState fileState = FileOperations.openFile(filename);
        final long fileTimestamp = fileState.getTimestamp();

        // Signal to the keydir that this file exists via
        // increment_file_id() with optional 2nd arg.  The NIF
        // needs to know the file exists, even if it contains only
        // tombstones or data errors.  Otherwise we risk of
        // reusing the file id for new data.

        keydir.incrementFileId(fileTimestamp);

        FileOperations.foldKeys(fileState, new FileOperations.KeyFoldFunction<KeyFoldMode>() {
            @Override
            public KeyFoldMode fold(boolean tombstone, byte[] key, long timestamp, long offset, long totalSize, KeyFoldMode dontcare) {
                if (tombstone ) {
                    try {
                        final byte[] tranformedKey = keyTransformer.apply(key);
                        keydir.keydirRemove(keyTransformer.apply(tranformedKey));
                    } catch (Throwable ex) {
                        System.err.printf("Invalid key on load %s: %s%n", key, ex);
                    }
                } else {
                    try {
                        final byte[] tranformedKey = keyTransformer.apply(key);
                        keydir.keydirPut(tranformedKey,
                                (int) fileTimestamp,
                                (int) totalSize,
                                offset,
                                timestamp,
                                System.currentTimeMillis(),
                                false);
                    } catch (Throwable ex) {
                        System.err.printf("Invalid key on load %s: %s%n", key, ex);
                    }
                }
                return null;
            }
        }, null, KeyFoldMode.RECOVERY);
        if (closeFile) {
            FileOperations.close(fileState);
        }
        acc.add(fileState);
        List<FileState> mergingList = new ArrayList<>();
        mergingList.add(fileState);
        mergingList.addAll(acc);
        return scanKeyFiles(rest, keydir, mergingList, closeFile, keyTransformer);
    }

    private static Map<Boolean, List<Path>> readableAndSetuidFiles(String dirname) {
        // Check the write and/or merge locks to see what files are currently
        // being written to. Generate our list excepting those.
        final String writingFile = LockOperations.readActivefile(LockType.WRITE, dirname);
        final String mergingFile = LockOperations.readActivefile(LockType.WRITE, dirname);

        // Filter out files with setuid bit set: they've been marked for
        // deletion by an earlier *successful* merge.
        final List<Path> fs = listDataFiles(dirname, writingFile, mergingFile);

        final String writingFile2 = LockOperations.readActivefile(LockType.WRITE, dirname);
        final String mergingFile2 = LockOperations.readActivefile(LockType.WRITE, dirname);
        if (writingFile.equals(writingFile2) && mergingFile.equals(mergingFile2)) {
            return fs.stream().collect(Collectors.partitioningBy(f -> ! JBitCask.hasPendingDeleteBit(f)));
        } else {
            // Changed while fetching file list, retry
            return readableAndSetuidFiles(dirname);
        }
    }

    private static IO.BCFileLock pollForMergeLock(String dirname) throws InterruptedException {
        return pollForMergeLock(dirname, 20);
    }

    private static IO.BCFileLock pollForMergeLock(String dirname, int loop) throws InterruptedException {
        if (loop == 0) {
            throw new BitCaskError("Polling iteration exhausted 20");
        }
        try {
            return LockOperations.acquire(LockType.MERGE, dirname);
        } catch (IOException | LockOperations.AlreadyLockedException ex) {
            Thread.sleep(200);
            return pollForMergeLock(dirname, loop - 1);
        }
    }

    private static void purgeSetuidFiles(String dirname) throws IOException {
        final IO.BCFileLock writeLock;
        try {
            writeLock = LockOperations.acquire(LockType.WRITE, dirname);
        } catch (LockOperations.AlreadyLockedException | IOException ex) {
            //TODO use logger
            System.err.printf("Lock failed trying deleting stale merge input files from %s %s %n", dirname, ex);
            return;
        }

        try {
            final List<Path> dataFiles = listDataFiles(dirname, null, null);
            final List<Path> staleFs = dataFiles.stream()
                    .filter(JBitCask::hasPendingDeleteBit)
                    .collect(Collectors.toList());
            for (Path staleFile : staleFs) {
                FileOperations.delete(staleFile);
            }
            if (!staleFs.isEmpty()) {
                System.err.printf("Deleted %d stale merge input files from %s%n", staleFs.size(), dirname);
            }
        } /*catch (IOException ex) {
                System.err.printf("While deleting stale merge input files from %s %s%n", dirname, ex);
            } */finally {
            LockOperations.release(writeLock);
        }
    }

    // Versions of Bitcask prior to
    // https://github.com/basho/bitcask/pull/156 used the setuid bit to
    // indicate that the data file has been deleted logically and is
    // waiting for a physical delete from the 'bitcask_merge_delete' server.
    //
    // However, with PR 156, we change the lifecycle of a .data file: it's
    // possible to append tombstones during a merge to a file that is
    // pending deletion.  If that happens, the file system will clear the
    // setuid bit when the first append happens.  That's not good.
    //
    // Going forward, instead of using the setuid bit for pending delete
    // status, we'll use the 8#001 execution permission bit.
    // For backward compatibility, has_pending_delete_bit() will check for
    // both the old pending bit, 8#40000 (setuid), as well as 8#0001.

    private static void setPendingDeleteBit(Path file) throws IOException {
        int uid = (Integer) Files.getAttribute(file, "unix:uid");
        final int newUid = uid | 0b000000001;
        Files.setAttribute(file, "unix:uid", newUid);
    }

    private static boolean hasPendingDeleteBit(Path file) {
        try {
            int uid = (Integer) Files.getAttribute(file, "unix:uid");
            return (uid & 0b100000000001) != 0;
        } catch (IOException ex) {
            throw new BitCaskError(ex);
        }
    }

    private static List<Path> listDataFiles(String dirname, String writingFile, String mergingFile) {
        final List<FileOperations.TimeStampedFile> files = FileOperations.dataFileTimestamps(dirname);
        files.sort(FileOperations.TimeStampedFile::compareTo);

        List<Path> res = new ArrayList<>();
        for (FileOperations.TimeStampedFile tf : files) {
            if (writingFile != null && !tf.getFilename().toString().equals(writingFile)) {
                res.add(tf.getFilename());
            }
            if (mergingFile != null && !tf.getFilename().toString().equals(mergingFile)) {
                res.add(tf.getFilename());
            }
        }
        return res;
    }

    private static Function<byte[], byte[]> getKeyTransform() {
        // identity function
        return (byte[] key) -> key;
    }

    private static <T> T getOption(String propName, Properties opts) {
        return (T) opts.getOrDefault(propName, System.getenv(propName));
    }

    static boolean isTombstone(ByteBuffer data) {
        byte[] dst = new byte[TOMBSTONE_PREFIX.length()];
        data.mark().get(dst).reset();
        return TOMBSTONE_PREFIX.equals(new String(dst));
    }
}
