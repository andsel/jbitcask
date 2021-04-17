package org.dna.jbitcask;

import org.apache.commons.lang3.StringUtils;
import org.dna.jbitcask.FileOperations.FileState;
import org.dna.jbitcask.FileOperations.KeyFoldMode;
import org.dna.jbitcask.FileOperations.KeyValueRecord;
import org.dna.jbitcask.LockOperations.LockType;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.stream.Collectors;

import static org.dna.jbitcask.FileOperations.FileState.FRESH;

public class JBitCask {

    // Bitcask instance state
    private static class BitcaskState {
        private String dirname;
        private List<FileState> readFiles;
        private FileState writeFile; // <fd>|null|fresh
        private IO.BCFileLock writeLock;
        private long maxFileSize;
        private Options opts;
        private Keydir keydir;

        public BitcaskState(String dirname, List<FileState> readFiles, FileState writeFile,
                            IO.BCFileLock writeLock, long maxFileSize, Options opts, Keydir keydir) {
            this.dirname = dirname;
            this.readFiles = readFiles;
            this.writeFile = writeFile;
            this.writeLock = writeLock;
            this.maxFileSize = maxFileSize;
            this.opts = opts;
            this.keydir = keydir;
        }

        // copy constructor
        public BitcaskState(BitcaskState other) {
            this.dirname = other.dirname;
            this.readFiles = other.readFiles;
            this.writeFile = other.writeFile;
            this.writeLock = other.writeLock;
            this.maxFileSize = other.maxFileSize;
            this.keydir = other.keydir;
            this.opts = other.opts;
        }
    }

    private static class MergeState {
        private final String dirname;
        private final IO.BCFileLock mergeLock;
        private final long maxFileSize;
        private final List<FileState> inputFiles;
        private final Set<Long> inputFileIds;
        private final long minFileId;
        private List<FileState> tombstoneWriteFiles = new ArrayList<>();
        FileState outFile = FRESH; //fresh, will be created when needed
        private final MergeCoverage mergeCoverage;
        private final Keydir liveKeydir;
        private final Keydir delKeydir;
        private final long expiryTime;
        private final long expiryGraceTime;
        private final Function<byte[], byte[]> keyTransformer;
        private boolean readWriteP = false;
        private final Options opts;
        private List<FileState> deleteFiles = new ArrayList<>();

        public MergeState(String dirname, IO.BCFileLock mergeLock, long maxFileSize, List<FileState> inputFiles,
                          Set<Long> inputFileIds, long minFileId, MergeCoverage mergeCoverage, Keydir liveKeydir,
                          Keydir delKeydir, long expiryTime, long expiryGraceTime,
                          Function<byte[], byte[]> keyTransformer, Options opts) {
            this.dirname = dirname;
            this.mergeLock = mergeLock;
            this.maxFileSize = maxFileSize;
            this.inputFiles = inputFiles;
            this.inputFileIds = inputFileIds;
            this.minFileId = minFileId;
            this.mergeCoverage = mergeCoverage;
            this.liveKeydir = liveKeydir;
            this.delKeydir = delKeydir;
            this.expiryTime = expiryTime;
            this.expiryGraceTime = expiryGraceTime;
            this.keyTransformer = keyTransformer;
            this.opts = opts;
        }

        MergeState copyWithDeleteFile(List<FileState> deleteFiles) {
            final MergeState copiedState = new MergeState(this.dirname, this.mergeLock, this.maxFileSize,
                    this.inputFiles, this.inputFileIds, this.minFileId, this.mergeCoverage,
                    this.liveKeydir, this.delKeydir, this.expiryTime, this.expiryGraceTime,
                    this.keyTransformer, this.opts);
            copiedState.deleteFiles = deleteFiles;
            copiedState.tombstoneWriteFiles = this.tombstoneWriteFiles;
            copiedState.outFile = this.outFile;
            copiedState.readWriteP = this.readWriteP;
            return copiedState;
        }

        MergeState copyWithInputFiles(List<FileState> inputFiles) {
            final MergeState newState = new MergeState(this.dirname, this.mergeLock, this.maxFileSize,
                    inputFiles, this.inputFileIds, this.minFileId, this.mergeCoverage,
                    this.liveKeydir, this.delKeydir, this.expiryTime, this.expiryGraceTime,
                    this.keyTransformer, this.opts);
            newState.tombstoneWriteFiles = tombstoneWriteFiles;
            newState.outFile = outFile;
            newState.readWriteP = readWriteP;
            newState.deleteFiles = deleteFiles;
            return newState;
        }
    }

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

    static final int DIABOLIC_BIG_INT = 100;

    // Notice that tombstone version 1 and 2 are the same size, so not tested below
    static boolean isTombstoneSize(int s) {
        return s == TOMBSTONE0_SIZE || s == TOMBSTONE1_SIZE;
    }

    static final int OFFSETFIELD_V1 = 64;
    static final int TOMBSTONEFIELD_V2 = 1;
    static final int OFFSETFIELD_V2 = 63;
    static final int TSTAMPFIELD = 4; //32 bit
    static final int KEYSIZEFIELD = 2; // 16 bit
    static final int TOTALSIZEFIELD = 4; //32 bit
    static final int VALSIZEFIELD = 4; // 32 bit
    static final int CRCSIZEFIELD = 4; //32 bit
    static final int HEADER_SIZE = 14; // 4 + 4 + 2 + 4 bytes
    static final long MAXKEYSIZE = 0xFFFF;
    static final long MAXVALSIZE = Short.MAX_VALUE;
    static final long MAXOFFSET_V2 = 0x7FFF_FFFF_FFFF_FFFFL; //max 63-bit unsigned

    //for hintfile validation
    public static final int CHUNK_SIZE = 65535;
    public static final int MIN_CHUNK_SIZE = 1024;
    public static final int MAX_CHUNK_SIZE = 134217728;

    private BitcaskState state;

    private Function<byte[], byte[]> keyTransform;
    private byte tombstoneVersion;
    private boolean readWriteP;

    /**
     * A bitcask is a directory containing:
     * - One or more data files - {integer_timestamp}.bitcask.data
     * - A write lock - bitcask.write.lock (Optional)
     * - A merge lock - bitcask.merge.lock (Optional)
     * */
    public static JBitCask open(String dirname) throws IOException, InterruptedException, TimeoutException {
        return open(dirname, new Options(new Properties()));
    }

    public static JBitCask open(String dirname, Options opts) throws IOException, InterruptedException, TimeoutException {
        final Path path = Paths.get(dirname, "bitcask");
        if (!path.toFile().exists()) {
            throw new FileNotFoundException("bitcask directory doesn't exists: " + path);
        }

        FileState writingFile;

        // If the read_write option is set, attempt to release any stale write lock.
        // Do this first to avoid unnecessary processing of files for reading.
        if (opts.get(Options.READ_WRITE)) {
            // If the lock file is not stale, we'll continue initializing
            // and loading anyway: if later someone tries to write
            // something, that someone will get a write_locked exception.
            LockOperations.deleteStaleLock(LockType.WRITE, dirname);
            writingFile = FRESH;
        } else {
            writingFile = null;
        }

        // Get the max file size parameter from opts
        final Long maxFileSize = opts.get(Options.MAX_FILE_SIZE);

        // Get the number of seconds we are willing to wait for the keydir init to timeout
        final Long waitMillis = opts.get(Options.OPEN_TIMEOUT);
        final long waitTime = waitMillis / 1000;

        // Set the key transform for this cask
        Function<byte[], byte[]> keyTransformer = getKeyTransform(); //opts key_transform

        //Type of tombstone to write, for testing.
        byte tombstoneVersion = opts.get(Options.TOMBSTONE_VERSION); // 0 or 2

        // Loop and wait for the keydir to come available.
        boolean readWriteP = writingFile != null;
        boolean readWriteI;
        if (readWriteP)
            readWriteI = true;
        else
            readWriteI = false;

        final Keydir keydir = initKeydir(dirname, waitTime, readWriteP, keyTransformer);
        // Ensure that expiry_secs is in Opts and not just application env
        opts.add(Options.EXPIRY_SECS, opts.get(Options.EXPIRY_SECS));
        final JBitCask bc = new JBitCask();
        bc.state = new BitcaskState(dirname, new ArrayList<>(), writingFile, null, maxFileSize, opts, keydir);
        bc.keyTransform = keyTransformer;
        bc.tombstoneVersion = tombstoneVersion;
        bc.readWriteP = readWriteI;
        return bc;
    }

    /*
     * Initialize a keydir for a given directory.
     * */
    private static Keydir initKeydir(String dirname, long waitTimeSecs, boolean readWriteMode,
                                     Function<byte[], byte[]> keyTransformer) throws IOException, InterruptedException, TimeoutException {
        // Get the named keydir for this directory. If we get it and it's already
        // marked as ready, that indicates another caller has already loaded
        // all the data from disk and we can short-circuit scanning all the files.
        final FunctionResult<FunctionResult.Atom, Keydir> result = Keydir.create(dirname);
        if (result.getAtom() == FunctionResult.Atom.READY) {
            // A keydir already exists, nothing more to do here. We'll lazy
            // open files as needed.
            return result.getResult();
        } else {
            if (result.getResult() != null) {
                final Keydir keydir = result.getResult();
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
            LOG.error("scan_key_files:", ex);
            initKeydirScanKeyFiles(dirname, keydir, keyTransformer, count - 1);
        }
    }

    private static List<FileState> scanKeyFiles(List<Path> files, Keydir keydir, List<FileState> acc, boolean closeFile,
                                             Function<byte[], byte[]> keyTransformer) throws IOException, InterruptedException {
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

        FileOperations.foldKeys(fileState, new FileOperations.RecordFoldFunction<KeyFoldMode, FileOperations.KeyRecord>() {
            @Override
            public KeyFoldMode fold(FileOperations.KeyRecord r, long timestamp, FileOperations.PosInfo pos, KeyFoldMode dontcare) {
                if (r.isTombstone) {
                    try {
                        final byte[] tranformedKey = keyTransformer.apply(r.key);
                        keydir.keydirRemove(keyTransformer.apply(tranformedKey));
                    } catch (Throwable ex) {
                        System.err.printf("Invalid key on load %s: %s%n", r.key, ex);
                    }
                } else {
                    try {
                        final byte[] tranformedKey = keyTransformer.apply(r.key);
                        keydir.keydirPut(tranformedKey,
                                (int) fileTimestamp,
                                (int) pos.totalSize,
                                pos.offset,
                                timestamp,
                                System.currentTimeMillis(),
                                false);
                    } catch (Throwable ex) {
                        System.err.printf("Invalid key on load %s: %s%n", r.key, ex);
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

    private static List<Path> readableFiles(String dirname) {
        final Map<Boolean, List<Path>> res = readableAndSetuidFiles(dirname);
        return res.get(true);
    }

    private static Map<Boolean, List<Path>> readableAndSetuidFiles(String dirname) {
        // Check the write and/or merge locks to see what files are currently
        // being written to. Generate our list excepting those.
        final String writingFile = LockOperations.readActivefile(LockType.WRITE, dirname);
        final String mergingFile = LockOperations.readActivefile(LockType.MERGE, dirname);

        // Filter out files with setuid bit set: they've been marked for
        // deletion by an earlier *successful* merge.
        final List<Path> fs = listDataFiles(dirname, writingFile, mergingFile);

        final String writingFile2 = LockOperations.readActivefile(LockType.WRITE, dirname);
        final String mergingFile2 = LockOperations.readActivefile(LockType.MERGE, dirname);
        if (StringUtils.equals(writingFile, writingFile2) && StringUtils.equals(mergingFile, mergingFile2)) {
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
            LOG.error("Lock failed trying deleting stale merge input files from %s %s %n", dirname, ex);
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

    static boolean isTombstone(ByteBuffer data) {
        byte[] dst = new byte[TOMBSTONE_PREFIX.length()];
        data.mark().get(dst).reset();
        return TOMBSTONE_PREFIX.equals(new String(dst));
    }

    static boolean isTombstone(byte[] data) {
        return TOMBSTONE_PREFIX.equals(new String(data));
    }

    /**
     * Close a bitcask data store and flush any pending writes to disk.
     */
    public void close() throws IOException {
        if (state.writeFile != null && state.writeFile != FRESH) {
            //Cleanup the write file and associated lock
            FileOperations.closeForWriting(state.writeFile);
            LockOperations.release(state.writeLock);
        }
        // Manually release the keydir. If, for some reason, this failed GC would
        // still get the job done.
        state.keydir.release();

        // Clean up all the reading files
        FileOperations.closeAll(state.readFiles);
    }

    /**
     * Store a key and value in a Bitcask datastore.
     * */
    public void put(byte[] key, byte[] value) throws IOException, LockOperations.AlreadyLockedException, InterruptedException {
        // Make sure we have a file open to write
        if (this.state.writeFile == null) {
            throw new ReadOnlyError();
        }
        this.state = doPut(key, value, this.state, DIABOLIC_BIG_INT, null);
    }

    // Internal put - have validated that the file is opened for write
    // and looked up the state at this point
    private BitcaskState doPut(byte[] key, byte[] value, BitcaskState state, int retries, BitCaskError lastError)
            throws IOException, LockOperations.AlreadyLockedException, InterruptedException {
        if (retries == 0) {
            throw lastError;
        }
        final int valSize;
        if (isTombstone(value)) {
            valSize = tombstoneSizeForVersion();
        } else {
            valSize = value.length;
        }
        final FileOperations.WriteType res = FileOperations.checkWrite(state.writeFile, key, valSize, state.maxFileSize);
        BitcaskState state2;
        switch (res) {
            case WRAP -> state2 = wrapWriteFile(state);
            case FRESH -> {
                final IO.BCFileLock writeLock = LockOperations.acquire(LockType.WRITE, state.dirname);
                final FileState newWriteFile = FileOperations.createFile(state.dirname, state.opts, state.keydir);
                LockOperations.writeActivefile(writeLock, newWriteFile.getFilename());
                state.writeFile = newWriteFile;
                state.writeLock = writeLock;
                state2 = state;
            }
            case OK -> state2 = state;
            default -> throw new IllegalArgumentException();
        }
        final long timestamp = System.currentTimeMillis();
        final FileState writeFile0 = state2.writeFile;
        final long writeFileId = writeFile0.getTimestamp();
        BitcaskState state3;
        if (JBitCask.isTombstone(value)) {
            final Keydir.EntryProxy entry;
            try {
                entry = state2.keydir.get(key);
            } catch (KeyNotFoundError notfund) {
                return state2;
            }
            final int oldFileId = entry.fileId;
            if (oldFileId > writeFileId) {
                // A merge wrote this key in a file > current write file
                // Start a new write file > the merge output file
                state3 = wrapWriteFile(state2);
                return doPut(key, value, state3, retries - 1, new AlreadyExistsError());
            } else {
                final long oldTimestamp = entry.tstamp;
                final long oldOffset = entry.offset;
                final ByteBuffer tombstone = ByteBuffer.allocate(TOMBSTONE2_SIZE);
                tombstone.put(TOMBSTONE2_STR.getBytes()).putInt(oldFileId).flip();
                final FileOperations.WriteResult writeRes = FileOperations.write(state2.writeFile, key, tombstone.array(), timestamp);
                final FileState writeFile2 = writeRes.fileState;
                final int tombstoneSize = writeRes.size;
                state2.keydir.updateFstats((int) writeFile2.getTimestamp(), timestamp,
                        0, 0, 0, 0, tombstoneSize, true);
                try {
                    state2.keydir.keydirRemove(key, oldTimestamp, oldFileId, oldOffset);
                    state2.writeFile = writeFile2;
                    return state2;
                } catch (AlreadyExistsError err) {
                    // Merge updated the keydir after tombstone
                    // write.  beat us, so undo and retry in a
                    // new file.
                    FileState writeFile3 = FileOperations.unWrite(writeFile2);
                    state2.writeFile = writeFile3;
                    state3 = wrapWriteFile(state2);
                    return doPut(key, value, state3, retries - 1, new AlreadyExistsError());
                }
            }
        } else {
            // Replacing value from a previous file, so write tombstone for it.
            final Keydir.EntryProxy entry;
            try {
                entry = state2.keydir.get(key);
            } catch (KeyNotFoundError notfound) {
                state3 = new JBitCask.BitcaskState(state2);
                state3.writeFile = writeFile0;
                return writeAndKeydirPut(state3, key, value, timestamp, retries, System.currentTimeMillis(), 0, 0);
            }
            final int oldFileId = entry.fileId;
            if (oldFileId > writeFileId) {
                state3 = wrapWriteFile(state2);
                return doPut(key, value, state3, retries - 1, new AlreadyExistsError());
            } else {
                if (oldFileId < writeFileId) {
                    final ByteBuffer prevTombstone = ByteBuffer.allocate(TOMBSTONE2_SIZE);
                    prevTombstone.put(TOMBSTONE2_STR.getBytes()).putInt(oldFileId).flip();
                    final FileOperations.WriteResult writeRes = FileOperations.write(state2.writeFile, key, prevTombstone.array(), timestamp);
                    state3 = new JBitCask.BitcaskState(state2);
                    state3.writeFile = writeRes.fileState;
                } else {
                    state3 = state2;
                }
                final long oldOffset = entry.offset;
                return writeAndKeydirPut(state3, key, value, timestamp, retries, System.currentTimeMillis(), oldFileId, oldOffset);
            }
        }
    }

    private BitcaskState writeAndKeydirPut(BitcaskState state, byte[] key, byte[] value, long timestamp, int retries,
                                           long nowTimestamp, int oldFileId, long oldOffset) throws IOException, LockOperations.AlreadyLockedException, InterruptedException {
        final FileOperations.WriteResult writeResult = FileOperations.write(state.writeFile, key, value, timestamp);
        final int fileTimestamp = (int) writeResult.fileState.getTimestamp();

        try {
            state.keydir.keydirPut(key, fileTimestamp, writeResult.size, writeResult.offset,
                    timestamp, nowTimestamp, true, oldFileId, (int) oldOffset);
            final BitcaskState newState = new BitcaskState(state);
            newState.writeFile = writeResult.fileState;
            return newState;
        } catch (AlreadyExistsError e) {
            // Assuming the timestamps in the keydir are
            // valid, there is an edge case where the merge thread
            // could have rewritten this Key to a file with a greater
            // file_id. Rather than synchronize the merge/writer processes,
            // wrap to a new file with a greater file_id and rewrite
            // the key there.
            // We must undo the write here so a later partial merge
            // does not see it.
            // Limit the number of recursions in case there is
            // a different issue with the keydir.
            FileState writeFile3 = FileOperations.unWrite(writeResult.fileState);
            final BitcaskState newState = new BitcaskState(state);
            newState.writeFile = writeFile3;
            final BitcaskState state3 = wrapWriteFile(newState);
            return doPut(key, value, state3, retries - 1, new AlreadyExistsError());
        }
    }

    private BitcaskState wrapWriteFile(BitcaskState state) throws IOException, InterruptedException {
        final FileState writeFile = state.writeFile;
        final FileState lastWriteFile = FileOperations.closeForWriting(writeFile);
        final FileState newWriteFile = FileOperations.createFile(state.dirname, state.opts, state.keydir);
        LockOperations.writeActivefile(state.writeLock, newWriteFile.getFilename());

        state.writeFile = newWriteFile;
        state.readFiles.add(0, lastWriteFile);
        return state;
    }

    private int tombstoneSizeForVersion() {
        return switch (tombstoneVersion) {
            case 0 -> TOMBSTONE0_SIZE;
            case 2 -> TOMBSTONE2_SIZE;
            default -> throw new IllegalArgumentException("tombstone version must be 0 or 2, received: " + tombstoneVersion);
        };
    }

    /**
     * Retrieve a value by key from a bitcask datastore.
     * @throws KeyNotFoundError
     * */
    public byte[] get(byte[] key) throws IOException {
        return get(key, 2);
    }

    private byte[] get(byte[] key, int trycount) throws IOException {
        if (trycount == 0) {
            throw new NoFileError();
        }

        final Keydir.EntryProxy entryProxy = this.state.keydir.get(key);
        if (entryProxy.tstamp < expiryTime(state.opts)) {
            // Expired entry; remove from keydir
            try {
                this.state.keydir.keydirRemove(key, entryProxy.tstamp, entryProxy.fileId, entryProxy.offset);
                throw new KeyNotFoundError();
            } catch (AlreadyExistsError e) {
                // Updated since last read, try again.
                return get(key, trycount - 1);
            }
        } else {
            // HACK: Use a fully-qualified call to get_filestate/2 so that
            // we can intercept calls w/ Pulse tests.
            final FileState filestate;
            try {
                filestate = getFilestate(entryProxy.fileId, this.state);
            } catch (FileNotFoundException e) {
                // merging deleted file between keydir_get and here
                return get(key, trycount - 1);
            }
            try {
                final FileOperations.KeyValue keyValue = FileOperations.read(filestate, entryProxy.offset, entryProxy.totalSize);
                if (isTombstone(keyValue.value)) {
                    throw new KeyNotFoundError();
                }
                return keyValue.value;
            } catch (EOFException eofex) {
                throw new KeyNotFoundError();
            }
        }
    }

    // modify the state implace
    private FileState getFilestate(int fileId, BitcaskState state) throws IOException {
        return getFilestate(fileId, state.dirname, state.readFiles, FileOperations.OpenMode.READONLY);
    }

    // modify the state implace
    private FileState getFilestate(int fileId, MergeState state) throws IOException {
        return getFilestate(fileId, state.dirname, state.tombstoneWriteFiles, FileOperations.OpenMode.APPEND);
    }

    // eventually modify readFiles, so it's an output param
    private FileState getFilestate(int fileId, String dirname, List<FileState> readFiles, FileOperations.OpenMode openMode) throws IOException {
        final FileState res = searchStateByTimestamp(fileId, readFiles);
        if (res != null) {
            return res;
        }
        final Path filename = FileOperations.mkFilename(dirname, fileId);
        final FileState fileState;
        try {
            fileState = FileOperations.openFile(filename, openMode);
        } catch (FileNotFoundException fnfex) {
            // merge removed the file since the keydir_get
            throw fnfex;
        }

        readFiles.addAll(0, Collections.singletonList(fileState));
        return fileState;
    }

    private FileState searchStateByTimestamp(long timestamp, List<FileState> files) {
        for (FileState fs : files) {
            if (fs.getTimestamp() == timestamp) {
                return fs;
            }
        }
        return null;
    }

    private static int filestateTimestampCompare(FileState t1, FileState t2) {
        return Long.compare(t1.getTimestamp(), t2.getTimestamp());
    }

    /**
     * @return seconds
     * */
    private int expiryTime(Options opts) {
        final int expirySecs = opts.get(Options.EXPIRY_SECS);
        if (expirySecs <= 0) {
            return 0;
        }
        return (int) (System.currentTimeMillis() / 1000) - expirySecs;
    }

    /**
     * Merge several data files within a bitcask datastore
     * */
    public void merge(String dirname) throws IOException, InterruptedException {
        merge(dirname, new Options(new Properties()));
    }

    private void merge(String dirname, Options options) throws IOException, InterruptedException {
        merge(dirname, options, readableFiles(dirname), new ArrayList<>());
    }

    private void merge(String dirname, Options options, List<Path> filesToMerge, List<Path> expiredFiles) throws IOException, InterruptedException {
        if (filesToMerge.isEmpty()) {
            return;
        }
        // Filter the files to merge and ensure that they all exist. It's
        // possible in some circumstances that we'll get an out-of-date
        // list of files.
        final List<Path> filesToMerge0 = filesToMerge.stream().filter(f -> f.toFile().exists()).collect(Collectors.toList());
        final List<Path> expiredFiles0 = expiredFiles.stream().filter(f -> f.toFile().exists()).collect(Collectors.toList());
        doMerge(dirname, options, filesToMerge0, expiredFiles0);
    }

    enum MergeCoverage {
        FULL, PREFIX, PARTIAL
    }

    // Inner merge function, assumes that all files exist.
    private void doMerge(String dirname, Options options, List<Path> filesToMerge, List<Path> expiredFiles) throws IOException, InterruptedException {
        if (filesToMerge.isEmpty() && expiredFiles.isEmpty()) {
            return;
        }
        Function<byte[], byte[]> keyTransformer = getKeyTransform();

        // Try to lock for merging
        final IO.BCFileLock lock;
        try {
            lock = LockOperations.acquire(LockType.MERGE, dirname);
        } catch (LockOperations.AlreadyLockedException e) {
            throw new MergeLockedError();
        }

        // Get the live keydir
        final Keydir liveKeyDir;
        try {
            liveKeyDir = Keydir.maybeKeydirNew(dirname);
        } catch (NotReadyError notReadyErr) {
            // Someone else is loading the keydir, or this cask isn't open.
            // We'll bail here and try again later.
            LockOperations.release(lock);
            throw notReadyErr;
        }

        // Simplest case; a key dir is already available and
        // loaded. Go ahead and open just the files we wish to
        // merge
        final List<FileState> inFiles0 = new ArrayList<>(filesToMerge.size());
        for (Path f: filesToMerge) {
            // Handle open errors gracefully.
            try {
                final FileState fileState = FileOperations.openFile(f);
                inFiles0.add(fileState);
            } catch (BitCaskError | IOException ex) {
                // skip it
            }
        }
        final BitcaskState newState = new BitcaskState(dirname, null, null, null, 0, options, liveKeyDir);
        //TODO overwrite immediately the state?
        this.state = newState;

        List<FileState> inFiles2 = new ArrayList<>();
        List<FileState> inExpiredFiles = new ArrayList<>();
        for (FileState fs : inFiles0) {
            if (expiredFiles.contains(Path.of(fs.getFilename()))) {
                inExpiredFiles.add(fs);
            } else {
                inFiles2.add(fs);
            }
        }

        // Test to see if this is a complete or partial merge
        // We perform this test now because our efforts to open the input files
        // in the inFiles list loop above may have had an open
        // failure.  The open(2) shouldn't fail, except, of course, when it
        // does, e.g. EMFILE, ENFILE, the OS decides EINTR because "reasons", ...
        final List<Path> readableFiles = readableFiles(dirname);
        readableFiles.removeAll(expiredFiles);
        Collections.sort(readableFiles);

        final List<Path> filesToMergeNew = inFiles2.stream()
                .map(FileState::getFilename)
                .map(Path::of)
                .sorted()
                .collect(Collectors.toList());

        MergeCoverage mergeCoverage;
        if (filesToMergeNew.containsAll(readableFiles) && readableFiles.containsAll(filesToMergeNew)) {
            mergeCoverage = MergeCoverage.FULL;
        } else if (isPrefix(filesToMergeNew, readableFiles)) {
            mergeCoverage = MergeCoverage.PREFIX;
        } else {
            mergeCoverage = MergeCoverage.PARTIAL;
        }

        // This sort is very important. The merge expects to visit files in order
        // to properly detect current values, and could resurrect old values if not.
        List<FileState> inFiles = new ArrayList<>(inFiles2);
        inFiles.sort(Comparator.comparingLong(FileState::getTimestamp));
        final Set<Long> inFileIds = inFiles.stream()
                .map(fileState -> fileState.getTimestamp())
                .collect(Collectors.toSet());
        final long minFileId = readableFiles.stream()
                .map(FileOperations::fileTimestamp)
                .min(Long::compare)
                .orElse(1L);

        // Initialize the other keydirs we need.
        final Keydir delKeyDir = Keydir.keydirNew();

        // Initialize our state for the merge
        final MergeState state = new MergeState(dirname, lock, options.get(Options.MAX_FILE_SIZE), inFiles, inFileIds, minFileId, mergeCoverage,
                liveKeyDir, delKeyDir, options.get(Options.EXPIRY_TIME), options.get(Options.EXPIRY_GRACE_TIME), keyTransformer, options);

        // Finally, start the merge process
        final List<FileState> expiredFilesFinished = expiryMerge(inExpiredFiles, liveKeyDir, keyTransformer, new ArrayList<>());
        final MergeState state1 = mergeFiles(state);

        // Make sure to close the final output file
        if (state1.outFile != FRESH) {
            state1.outFile.close();
        }

        for (FileState tfile : state1.tombstoneWriteFiles) {
            tfile.close();
        }

        // Close the original input files, schedule them for deletion,
        // close keydirs, and release our lock
        final List<FileState> toClose = new ArrayList<>(state.inputFiles);
        toClose.addAll(expiredFilesFinished);
        for (FileState fs : toClose) {
            fs.close();
        }
        final long iterGeneration = liveKeyDir.getIterGeneration();
        final List<FileState> delFiles = new ArrayList<>(state1.deleteFiles);
        delFiles.addAll(expiredFilesFinished);
        final List<String> fileNames = delFiles.stream().map(FileState::getFilename).collect(Collectors.toList());
        final List<Long> delIds = delFiles.stream().map(FileState::getTimestamp).collect(Collectors.toList());
        for (Long delId : delIds) {
            liveKeyDir.setPendingDelete(delId);
        }
        for (String fileName : fileNames) {
            setPendingDeleteBit(Path.of(fileName));
        }

        deferDelete(dirname, iterGeneration, fileNames);

        // Explicitly release our keydirs instead of waiting for GC
        liveKeyDir.release();
        delKeyDir.release();
        LockOperations.release(lock);
    }

    private void deferDelete(String dirname, long iterGeneration, List<String> fileNames) {
        // TODO maybe do it in a background thread
    }

    // Internal merge function for cache_merge functionality.
    private List<FileState> expiryMerge(List<FileState> files, Keydir liveKeyDir, Function<byte[], byte[]> keyTransformer,
                                        List<FileState> acc) {
        if (files.isEmpty()) {
            return acc;
        }
        final FileState file = files.remove(0);
        final long fileId = file.getTimestamp();

        FileOperations.RecordFoldFunction<KeyFoldMode, FileOperations.KeyRecord> foldFunc = new FileOperations.RecordFoldFunction<KeyFoldMode, FileOperations.KeyRecord>() {
            @Override
            public KeyFoldMode fold(FileOperations.KeyRecord r, long timestamp, FileOperations.PosInfo pos, KeyFoldMode acc) {
                if (r.isTombstone) {
                    return acc;
                }
                try {
                    final byte[] kt = keyTransformer.apply(r.key);
                    liveKeyDir.keydirRemove(kt, timestamp, (int) fileId, pos.offset);
                } catch (Exception ex) {
                    LOG.error("Invalid key on merge {}", r.key, ex);
                }
                return acc;
            }
        };

        try {
            FileOperations.foldKeys(file, foldFunc, KeyFoldMode.DEFAULT);
            LOG.info("All keys expired in: {} scheduling file for deletion", file.getFilename());
            acc.add(file);
        } catch (Exception ex) {
            LOG.error("Error folding keys for {}", file.getFilename(), ex);
        }
        expiryMerge(files, liveKeyDir, keyTransformer, acc);
        return files;
    }

    private MergeState mergeFiles(MergeState state) throws IOException, InterruptedException {
        if (state.inputFiles.isEmpty()) {
            return state;
        }
        final FileState file = state.inputFiles.remove(0);
        final List<FileState> rest = state.inputFiles;
        final long fileId = file.getTimestamp();

        FileOperations.RecordFoldFunction<MergeState, KeyValueRecord> foldFunc = new FileOperations.RecordFoldFunction<MergeState, KeyValueRecord>() {

            @Override
            public MergeState fold(KeyValueRecord r, long timestamp, FileOperations.PosInfo pos, MergeState state0) throws IOException, InterruptedException {
                byte[] key0 = r.key;
                byte[] value = r.value;
                final byte[] key;
                try {
                    key = keyTransform.apply(key0);
                } catch (BitCaskError err) {
                    LOG.error("Invalid key on merge {}", key0, err);
                    return state0;
                }

                return mergeSingleEntry(key, value, timestamp, fileId, pos, state0);
            }
        };

        final MergeState state1 = FileOperations.fold(file, foldFunc, state);
        MergeState state2;
        try {
            List<FileState> delFiles = new ArrayList<>(state1.deleteFiles);
            delFiles.add(0, file);
            state2 = state1.copyWithDeleteFile(delFiles);
        } catch (Exception e) {
            LOG.error("merge_files: skipping file {} in {}", file.getFilename(), state.dirname, e);
            state2 = state;
        }
        return mergeFiles(state2.copyWithInputFiles(rest));
    }

    private MergeState mergeSingleEntry(byte[] key, byte[] value, long timestamp, long fileId,
                                        FileOperations.PosInfo pos, MergeState state) throws IOException, InterruptedException {
        try {
            final boolean outOfDate = outOfDate(state, key, timestamp, fileId, pos, state.expiryTime, false,
                    List.of(state.liveKeydir, state.delKeydir));
            if (outOfDate) {
                // Value in keydir is newer, so drop ... except! ...
                // We aren't done yet: V might be a tombstone, which means
                // that we might have to merge it forward.  The func below
                // is safe (does nothing) if V is not really a tombstone.
                return mergeSingleTombstone(key, value, timestamp, fileId, pos.offset, state);
            } else {
                // Either a current value or a tombstone with nothing in the keydir
                // but an entry in the del keydir because we've seen another during
                // this merge.
                if (isTombstone(value)) {
                    // We have seen a tombstone for this key before, but this
                    // one is newer than that one.
                    state.delKeydir.keydirPut(key, (int) fileId, 0, pos.offset, timestamp, System.currentTimeMillis(), false);
                    if (state.mergeCoverage == MergeCoverage.PARTIAL) {
                        return innerMergeWrite(key, value, timestamp, fileId, pos.offset, state);
                    } else {
                        // Full or prefix merge, safe to drop the tombstone
                        return state;
                    }
                } else {
                    state.delKeydir.keydirRemove(key);
                    return innerMergeWrite(key, value, timestamp, fileId, pos.offset, state);
                }
            }
        } catch (ExpiredError expired) {
            // Note: we drop a tombstone if it expired. Under normal
            // circumstances it's OK as any value older than that has expired
            // too and you wouldn't see values coming back to life after a
            // merge and reopen.  However with a clock going back in time,
            // and badly timed quick merges you could end up seeing an old
            // value after we drop a tombstone that has a lower timestamp
            // than a value that was actually written before. Likely that other
            // value would expire soon too, but...

            // Remove only if this is the current entry in the keydir
            state.liveKeydir.keydirRemove(key, timestamp, (int) fileId, pos.offset);
            return state;
        } catch (NotFoundError notFound) {
            // First tombstone seen for this key during this merge
            return mergeSingleTombstone(key, value, timestamp, fileId, pos.offset, state);
        }
    }

    private MergeState mergeSingleTombstone(byte[] key, byte[] value, long timestamp, long fileId, long offset,
                                            MergeState state) throws IOException, InterruptedException {
        try {
            final TombstoneResp res = tombstoneContext(value);
            final int oldFileId = res.fileId;
            switch(res.atom) {
                case BEFORE:
                    if (state.minFileId > oldFileId) {
                        return state;
                    } else {
                        return innerMergeWrite(key, value, timestamp, fileId, offset, state);
                    }
                case AT:
                    // Tombstone has info on deleted value
                    if (state.inputFileIds.contains(oldFileId)) {
                        // Merge will waste the original value too. Drop it.
                        return state;
                    }

                    // Append to original file
                    try {
                        final FileState tfile = getFilestate(oldFileId, state);
                        // Original file still around, append to it
                        final FileOperations.WriteResult writeResult = FileOperations.write(tfile, key, value, timestamp);
                        final FileState tfile2 = writeResult.fileState;
                        final int tsize = writeResult.size;
                        state.liveKeydir.updateFstats(oldFileId, timestamp, 0L, 0, 0, 0, tsize, false);
                        // implace change
                        final List<FileState> tfiles2 = state.tombstoneWriteFiles.stream().map(fs ->
                        {
                            if (fs.getFilename().equals(tfile.getFilename()))
                                return tfile2;
                            else
                                return fs;
                        }).collect(Collectors.toList());
                        state.tombstoneWriteFiles = tfiles2;
                        return state;
                    } catch (FileNotFoundException fnfex) {
                        // Original file is gone, safe to drop
                        return state;
                    }
            }
        } catch (UndefinedError undefined) {
            // Version 1 tombstone, no info on deleted value
            // Not in keydir and not already deleted.
            // Remember we deleted this already during this merge.
            state.delKeydir.keydirPut(key, (int) fileId, 0, offset, timestamp, System.currentTimeMillis(), true);
            if (state.mergeCoverage == MergeCoverage.PREFIX) {
                byte[] v2 = new byte[TOMBSTONE1_SIZE];
                System.arraycopy(TOMBSTONE1_BIN, 0, v2, 0, TOMBSTONE1_BIN.length);
                v2[TOMBSTONE1_BIN.length] = (byte) ((fileId & 0xFF000000) >> 24);
                v2[TOMBSTONE1_BIN.length + 1] = (byte) ((fileId & 0x00FF0000) >> 16);
                v2[TOMBSTONE1_BIN.length + 2] = (byte) ((fileId & 0x0000FF00) >> 8);
                v2[TOMBSTONE1_BIN.length + 3] = (byte) (fileId & 0x000000FF);
                // Merging only some files, forward tombstone
                return innerMergeWrite(key, v2, timestamp, fileId, offset, state);
            }
            // Full or prefix merge, so safe to drop tombstone
            return state;
        } catch (NoTombstoneError noTombstone) {
            // Regular value not currently in keydir, ignore
            return state;
        }

        throw new RuntimeException("mergeSingleTombstone MUST never be here");
    }

    private MergeState innerMergeWrite(byte[] key, byte[] value, long timestamp, long oldFileId, long oldOffset,
                                       MergeState state) throws IOException, InterruptedException {
        // write a single item while inside the merge process

        // See if it's time to rotate to the next file
        // Close the current output file
        // Start our next file and update state
        // create the output file and take the lock.
        MergeState state1 = switch (FileOperations.checkWrite(state.outFile, key, value.length, state.maxFileSize)) {
            case WRAP -> {
                state.outFile.close();
                final FileState newFile = FileOperations.createFile(state.dirname, state.opts, state.liveKeydir);
                final String newFileName = newFile.getFilename();
                LockOperations.writeActivefile(state.mergeLock, newFileName);
                state.outFile = newFile;
                yield state;
            }
            case OK -> state;
            case FRESH -> {
                final FileState newFile = FileOperations.createFile(state.dirname, state.opts, state.liveKeydir);
                final String newFileName = newFile.getFilename();
                LockOperations.writeActivefile(state.mergeLock, newFileName);
                state.outFile = newFile;
                yield state;
            }
        };
        final FileOperations.WriteResult writeRes = FileOperations.write(state.outFile, key, value, timestamp);
        final long outFileId = writeRes.fileState.getTimestamp();
        if (outFileId <= oldFileId) {
            throw new InvariantViolationError(key, value, oldFileId, oldOffset, outFileId, writeRes.offset);
        }

        FileState outFile2;
        if (!isTombstone(value)) {
            // Update live keydir for the current out
            // file. It's possible that someone else may have written
            // a newer value whilst we were processing ... and if
            // they did, we need to undo our write here.
            try {
                state1.liveKeydir.keydirPut(key, (int) outFileId, writeRes.size, writeRes.offset, timestamp,
                        System.currentTimeMillis(), false, (int) oldFileId, (int) oldOffset);
                outFile2 = writeRes.fileState;
            } catch (AlreadyExistsError alreadyExists) {
                outFile2 = FileOperations.unWrite(writeRes.fileState);
            }
        } else {
            try {
                final Keydir.EntryProxy entry = state1.liveKeydir.get(key);
                // New value written, undo
                outFile2 = FileOperations.unWrite(writeRes.fileState);
            } catch (KeyNotFoundError notFound) {
                // Update timestamp and total bytes stats
                state1.liveKeydir.updateFstats((int) outFileId, timestamp, 0,0, 0, 0, writeRes.size, true);
                // Still not there, tombstone write is cool
                outFile2 = writeRes.fileState;
            }

        }
        state1.outFile = outFile2;
        return state1;
    }

    enum TombstoneAtom {BEFORE, AT}

    final static class TombstoneResp {
        final TombstoneAtom atom;
        final int fileId;

        TombstoneResp(TombstoneAtom atom, int fileId) {
            this.atom = atom;
            this.fileId = fileId;
        }
    }

    private TombstoneResp tombstoneContext(byte[] value) {
        if (Arrays.equals(TOMBSTONE0_STR.getBytes(), value)) {
            throw new UndefinedError();
        }
        if (value.length != TOMBSTONE1_SIZE) {
            throw new NoTombstoneError();
        }
        byte[] tombArray = new byte[TOMBSTONE1_BIN.length];
        System.arraycopy(value, 0, tombArray, 0, TOMBSTONE1_BIN.length);
        byte[] filedIdArray = new byte[4]; // 32 bit
        System.arraycopy(value, TOMBSTONE1_BIN.length, filedIdArray, 0, 4);
        int fileId = filedIdArray[0] & filedIdArray[1] << 8 & filedIdArray[2] << 16 & filedIdArray[3] << 24;
        if (Arrays.equals(tombArray, TOMBSTONE1_BIN)) {
            return new TombstoneResp(TombstoneAtom.BEFORE, fileId);
        }
        if (Arrays.equals(tombArray, TOMBSTONE2_BIN)) {
            return new TombstoneResp(TombstoneAtom.AT, fileId);
        }
        throw new NoTombstoneError();
    }

    /**
     * @throws NotFoundError when keydirs is empty and everFound is false.
     * @throws ExpiredError when timestamp is older than expiryTime.
     * */
    private boolean outOfDate(MergeState state, byte[] key, long timestamp, long fileId, FileOperations.PosInfo pos,
                              long expiryTime, boolean everFound, List<Keydir> keydirs) {
        if (keydirs.isEmpty()) {
            // if we ever found it, and none of the entries were out of date,
            // then it's not out of date
            if (everFound) {
                return false;
            } else {
                throw new NotFoundError();
            }
        }
        if (timestamp < expiryTime) {
            throw new ExpiredError();
        }

        final Keydir keydir = keydirs.remove(0);
        final Keydir.EntryProxy bitcaskEntry;
        try {
            bitcaskEntry = keydir.get(key);
        } catch (NotFoundError notFound) {
            return outOfDate(state, key, timestamp, fileId, pos, expiryTime, everFound, keydirs);
        }

        if (bitcaskEntry.tstamp == timestamp) {
            // Exact match. In this situation, we use the file
            // id and offset as a tie breaker.
            // The assumption is that newer values are written to
            // higher file ids and offsets, even in the case of a merge
            // racing with the write process, as the writer thread
            // will detect that and retry the write to an even higher
            // file id.
            if (bitcaskEntry.fileId > fileId) {
                return true;
            }
            if (bitcaskEntry.fileId == fileId) {
                if (bitcaskEntry.offset > pos.offset) {
                    return true;
                } else {
                    return outOfDate(state, key, timestamp, fileId, pos, expiryTime, true, keydirs);
                }
            } else {
                // At this point the following conditions are true:
                // The file_id in the keydir is older (<) the file
                // id we're currently merging...
                //
                // OR:
                //
                // The file_id in the keydir is the same (==) as the
                // file we're merging BUT the offset the keydir has
                // is older (<=) the offset we are currently
                // processing.
                //
                // Thus, we are NOT out of date. Check the
                // rest of the keydirs to ensure this
                // holds true.
                return outOfDate(state, key, timestamp, fileId, pos, expiryTime, true, keydirs);
            }
        } else if (bitcaskEntry.tstamp < timestamp) {
            // Not out of date -- check rest of the keydirs
            return outOfDate(state, key, timestamp, fileId, pos, expiryTime, true, keydirs);
        } else {
            // Out of date!
            return true;
        }
    }


    private boolean isPrefix(List<Path> prefix, List<Path> target) {
        if (target.size() < prefix.size()) {
            return false;
        }
        int size = prefix.size();
        for (int i = 0; i < size; i++) {
            if (!prefix.get(i).equals(target.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Delete a key from a bitcask datastore.
     * */
    public void delete(byte[] key) throws InterruptedException, IOException, LockOperations.AlreadyLockedException {
        put(key, TOMBSTONE2_BIN);
    }

    /**
     * List all keys in a bitcask datastore.
     * */
    public List<byte[]> listKeys() {
        final BiFunction<List<byte[]>, Keydir.KeydirEntry, List<byte[]>> fun = (acc, entry) -> {
            acc.add(entry.key);
            return acc;
        };
        return foldKeys(fun, new ArrayList<>());
    }

    /**
     * Fold over all keys in a bitcask datastore.
     * Must be able to understand the bitcask_entry record form.
     * */
    public List<byte[]> foldKeys(BiFunction<List<byte[]>, Keydir.KeydirEntry, List<byte[]>> fun, List<byte[]> acc) {
        long maxAge = state.opts.get(Options.MAX_FOLD_AGE) * 1000; // convert from ms to us
        int maxPuts = state.opts.get(Options.MAX_FOLD_PUTS);
        return foldKeys(fun, acc, maxAge, maxPuts, false);
    }

    /**
     * Fold over all keys in a bitcask datastore with limits on how out of date the keydir is allowed to be.
     * Must be able to understand the bitcask_entry record form.
     * */
    protected List<byte[]> foldKeys(BiFunction<List<byte[]>, Keydir.KeydirEntry, List<byte[]>> fun, List<byte[]> acc0,
                                    Long maxAge, Integer maxPuts, boolean seeTombstones) {
        if (maxAge != null && maxAge < 0) {
            throw new IllegalArgumentException("maxAge must be positive");
        }
        if (maxPuts != null && maxPuts < 0) {
            throw new IllegalArgumentException("maxPuts must be positive");
        }
        final long expiryTime = state.opts.get(Options.EXPIRY_TIME);

        final BiFunction<List<byte[]>, Keydir.KeydirEntry, List<byte[]>> realFun = (acc, entry) -> {
            final byte[] key = entry.key;
            if (entry.tstamp < expiryTime) {
                return acc;
            }
            if (isTombstoneSize(entry.totalSize - (HEADER_SIZE + key.length))) {
                // might be a deleted record, so check
                try {
                    get(key);
                    return fun.apply(acc, entry);
                } catch (NotFoundError notFound) {
                    if (!seeTombstones) {
                        return acc;
                    } else {
// TODO                        return fun.apply({tombstone, entry}, acc);
                        return fun.apply(acc, entry);
                    }
                } catch (IOException ioex) {
                    throw new RuntimeException(ioex);
                }
            } else {
                return fun.apply(acc, entry);
            }
        };

        return state.keydir.keydirFold(realFun, acc0, maxAge, maxPuts);
    }

    /**
     * Force any writes to sync to disk.
     * */
    public void sync() throws IOException {
        if (state.writeFile == null || state.writeFile == FRESH) {
            return;
        }
        state.writeFile.sync();
    }
}
