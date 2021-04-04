package org.dna.jbitcask;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

class FileOperations {

    static final short HINT_RECORD_SZ = 18;// Tstamp(4) + KeySz(2) + TotalSz(4) + Offset(8)

    enum OpenMode {
        READONLY, APPEND, READ_WRITE
    }

    /**
     * Use only after merging, to permanently delete a data file.
     */
    static void delete(Path file) {
        file.toFile().delete();
        if (hasHintFile(file)) {
            new File(hintFilename(file)).delete();
        }
    }

    private static boolean hasHintFile(Path file) {
        return new File(hintFilename(file)).exists();
    }

    private static String hintFilename(Path file) {
        if (file.endsWith(".data")) {
            final String fullpath = file.toString();
            return fullpath.replace(".data", "") + ".hint";
        }
        return file.toString();
    }

    private static String hintfileName(String filename) {
        if (filename.endsWith(".data")) {
            return filename.replace(".data", "") + ".hint";
        }
        return filename;
    }

    private static String hintfileName(FileState state) {
        return hintfileName(state.filename);
    }

    static class FileState {
        static final FileState FRESH = new FileState(OpenMode.APPEND, "FRESH", -1, null, -1);

        private final OpenMode mode; //File mode: read_only, read_write
        private final String filename;
        private final long timestamp; //Tstamp portion of filename
        private final FileChannel fd;
        private final long ofs; //Current offset for writing
        private final long hintcrc; //CRC-32 of current hint
        private final FileChannel hintfd; //File handle for hints
        private long lastOfs = 0; // Last offset written to data file
        private long lastHintBytes = 0; // Last # bytes written to hint file
        private int lastHintCrc = 0; // CRC-32 of current hint prior to last write

        FileState(OpenMode mode, String filename, long tstamp, FileChannel fd, long ofs) {
            this(mode, filename, tstamp, fd, ofs, 0, null);
        }

        FileState(OpenMode mode, String filename, long tstamp, FileChannel fd, long ofs, long hintcrc, FileChannel hintfd) {
            this.mode = mode;
            this.filename = filename;
            this.timestamp = tstamp;
            this.fd = fd;
            this.ofs = ofs;
            this.hintcrc = hintcrc;
            this.hintfd = hintfd;
        }

        public FileState(OpenMode mode, String filename, long tstamp, FileChannel fd, long ofs, long hintcrc, FileChannel hintfd,
                         long lastOfs, long lastHintBytes, int lastHintCrc) {
            this.mode = mode;
            this.filename = filename;
            this.timestamp = tstamp;
            this.fd = fd;
            this.ofs = ofs;
            this.hintcrc = hintcrc;
            this.hintfd = hintfd;
            this.lastOfs = lastOfs;
            this.lastHintBytes = lastHintBytes;
            this.lastHintCrc = lastHintCrc;
        }


        public long getTimestamp() {
            return timestamp;
        }

        public boolean hasFileDescriptor() {
            return fd != null;
        }

        public String getFilename() {
            return filename;
        }

        public void close() throws IOException {
            fd.close();
        }

        public void sync() throws IOException {
            fd.force(true);
        }
    }

    /**
     * Open a new file for writing.
     * Called on a Dirname, will open a fresh file in that directory.
     */
    static FileState createFile(String dirname, Options opts, Keydir keydir) throws IOException, InterruptedException {
        final IO.BCFileLock lock;
        try {
            lock = getCreateLock(dirname);
        } catch (IOException | InterruptedException ex) {
            throw ex;
        }
        try {
            final long newest = keydir.incrementFileId();
            final String filename = mkFilename(dirname, newest);
            ensureDir(filename);

            // Check for o_sync strategy and add to opts
            Set<OpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(StandardOpenOption.WRITE);
            if (opts.get(Options.SYNC_STRATEGY)) {
                options.add(StandardOpenOption.SYNC);
            }
            // Try to open the lock file
            final FileChannel fd = FileChannel.open(Path.of(filename), options);
            final FileChannel hintFD = openHintFile(Path.of(filename), options.toArray(new OpenOption[0]));
            return new FileState(OpenMode.READ_WRITE, filename, fileTimestamp(filename), fd, 0, 0, hintFD);
        } finally {
            LockOperations.release(lock);
        }
    }

    private static void ensureDir(String filename) throws IOException {
        final Path fullPath = Paths.get(filename);
        Files.createDirectories(fullPath.getParent());
    }

    static String mkFilename(String dirname, long newest) {
        return Paths.get(dirname, newest + ".bitcask.data").toString();
    }

    private static IO.BCFileLock getCreateLock(String dirname) throws IOException, InterruptedException {
        for (int i = 100; i > 0; i--) {
            Thread.sleep(100 - i);
            try {
                final IO.BCFileLock lock = LockOperations.acquire(LockOperations.LockType.CREATE, dirname);
                return lock;
            } catch (LockOperations.AlreadyLockedException e) {
                return getCreateLock(dirname);
            } catch (IOException e) {
                throw new IOException(e);
            }
        }
        return null;
    }

    /**
     * Open an existing file for reading.
     * Called with fully-qualified filename.
     */
    static FileState openFile(Path filename) throws IOException {
        return openFile(filename, OpenMode.READONLY);
    }

    /**
     * throw BitCaskError if openMode is not valid (READONLY|APPEND)
     */
    static FileState openFile(Path filename, OpenMode openMode) throws IOException {
        if (openMode == OpenMode.READONLY) {
            return new FileState(OpenMode.READONLY, filename.getFileName().toString(), fileTimestamp(filename),
                    FileChannel.open(filename), 0);
        } else if (openMode == OpenMode.APPEND) {
            Set<OpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.APPEND);
            final FileChannel fd = FileChannel.open(filename, options);
            final long offset = fd.position();
            if (offset == 0) {
                // File was deleted and we just opened a new one, undo.
                fd.close();
                Files.delete(filename);
                throw new FileNotFoundException("Undoing opening a just delete file");
            }
            try {
                final FileAndCRC fileAndCRC = reopenHintfile(filename);
                return new FileState(OpenMode.READ_WRITE, filename.getFileName().toString(), fileTimestamp(filename),
                        fd, offset, fileAndCRC.crc, fileAndCRC.file);
            } catch (FileNotFoundException | BitCaskError e) {
                fd.close();
                throw new FileNotFoundException("Error in opening " + filename);
            }
        }
        throw new BitCaskError("openFile doesn't expect " + openMode);
    }

    static class FileAndCRC {
        final FileChannel file;
        final long crc;

        public FileAndCRC(FileChannel file, long crc) {
            this.file = file;
            this.crc = crc;
        }
    }

    /**
     * Re-open hintfile for appending.
     * throws BitCaskError if file can't be open
     *
     * @return
     */
    private static FileAndCRC reopenHintfile(Path filename) throws IOException {
        final FileChannel hintFD = openHintFile(filename);
        final String hintfileName = hintfileName(filename.toString());
        final long hintSize = hintFD.size();
        hintFD.position(hintSize);
        final long newPosition = hintFD.position();
        if (newPosition == 0) {
            hintFD.close();
            Files.delete(Path.of(hintfileName));
            throw new FileNotFoundException("Undoing opening in reopenHintfile");
        }
        final long crc = prepareHintfileForAppend(hintFD);
        return new FileAndCRC(hintFD, crc);
    }

    /**
     * Removes the final CRC record so more records can be added to the file.
     */
    private static long prepareHintfileForAppend(FileChannel hintFD) throws IOException {
        try {
            final long crc = readCrc(hintFD);
            // hintFD.truncate(0);
            return crc;
        } catch (IOException e) {
            hintFD.close();
            throw new IOException("error in reading CRC of " + hintFD, e);
        }
    }

    /**
     * throws BitCaskError if file can't be open
     */
    private static FileChannel openHintFile(Path filename, OpenOption... options) throws IOException {
        final String hintFilename = hintFilename(filename);
        for (int i = 0; i < 10; i++) {
            try {
                return FileChannel.open(Path.of(hintFilename), options);
            } catch (FileAlreadyExistsException ex) {
                // continue the loop
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        throw new BitCaskError("couldnt_open_hintfile");
    }

    static class TimeStampedFile implements Comparable<TimeStampedFile> {
        private final long timestamp;
        private final Path filename;

        public TimeStampedFile(long timestamp, Path filename) {
            this.timestamp = timestamp;
            this.filename = filename;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimeStampedFile that = (TimeStampedFile) o;
            return timestamp == that.timestamp && filename.equals(that.filename);
        }

        public Path getFilename() {
            return filename;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, filename);
        }

        @Override
        public int compareTo(TimeStampedFile o) {
            return Long.compare(this.timestamp, o.timestamp);
        }
    }

    /**
     * Build a list of {tstamp, filename} for all files in the directory that match our regex.
     */
    static List<TimeStampedFile> dataFileTimestamps(String dirname) {
        //filename are in format <timestamp>.<something>.data
        final File dir = new File(dirname);
        final String[] allFiles = dir.list();
        List<TimeStampedFile> res = new ArrayList<>();

        final Pattern pattern = Pattern.compile("^(\\d*)\\..*\\.data$");
        for (String filename : allFiles) {
            Matcher m = pattern.matcher(filename);
            if (m.matches()) {
                final long timestamp = Long.parseLong(m.group(1));
                res.add(new TimeStampedFile(timestamp, Path.of(dirname, filename)));
            }
        }
        return res;
    }

    static long fileTimestamp(String filename) {
        return Long.parseLong(filename.replace(".bitcask.data", ""));
    }

    static long fileTimestamp(Path filename) {
        return fileTimestamp(filename.getFileName().toString());
    }

    static long fileTimestamp(FileState fileState) {
        return fileState.timestamp;
    }

    static class HintfileData {
        private final Long dataSize;
        private final String hintFile;

        public HintfileData(Long dataSize, String hintFile) {
            this.dataSize = dataSize;
            this.hintFile = hintFile;
        }
    }

    static class KeyData {
        private final String filename;
        private final long fTStamp;
        private final long offset;
        private final long crcSkipCount;

        KeyData(String filename, long fTStamp, long offset, long crcSkipCount) {
            this.filename = filename;
            this.fTStamp = fTStamp;
            this.offset = offset;
            this.crcSkipCount = crcSkipCount;
        }
    }

    enum KeyFoldMode {
        DATAFILE, HINTFILE, DEFAULT, RECOVERY;
    }

    static class KeyRecord {
        final boolean isTombstone;
        final byte[] key;

        public KeyRecord(boolean isTombstone, byte[] key) {
            this.isTombstone = isTombstone;
            this.key = key;
        }
    }

    static class KeyValueRecord {
        final byte[] key;
        final byte[] value;

        public KeyValueRecord(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    @FunctionalInterface
    public interface RecordFoldFunction<A, R> {
        A fold(R record, long timestamp, PosInfo pos, A acc) throws IOException, InterruptedException;
    }

    static class BytesFoldFunResult<D, A> {
        private final A acc;
        private final Long consumed;
        private final D data;
        private final Type type;

        public BytesFoldFunResult(Type type, A acc, long consumed) {
            this(type, acc, consumed, null);
        }

        public BytesFoldFunResult(Type type, A acc) {
            this(type, acc, null, null);
        }

        enum Type {MORE, DONE}

        BytesFoldFunResult(Type type, A acc, Long consumed, D data) {
            this.type = type;
            this.acc = acc;
            this.consumed = consumed;
            this.data = data;
        }

        boolean containOnlyAcc() {
            return consumed == null;
        }
    }

    @FunctionalInterface
    public interface BytesFoldFunction<D, A, R> {
        BytesFoldFunResult<D, A> fold(ByteBuffer bytes, RecordFoldFunction<A, R> keyFoldFun, A acc, long totalSize, D args) throws IOException, InterruptedException;
    }

    static KeyFoldMode foldKeys(FileState state, RecordFoldFunction<KeyFoldMode, KeyRecord> foldFunc, KeyFoldMode acc) {
        // TODO if state == :fresh return acc
        return foldKeys(state, foldFunc, KeyFoldMode.DEFAULT);
    }

    static <A> A foldKeys(FileState state, RecordFoldFunction<A, KeyRecord> foldFunc, A acc, KeyFoldMode mode) throws IOException, InterruptedException {
        if (mode == KeyFoldMode.DATAFILE) {
            return foldKeysLoop(state, 0, foldFunc, acc);
        }
        if (mode == KeyFoldMode.HINTFILE && state.hasFileDescriptor()) {
            return foldHintfile(state, foldFunc, acc);
        }
        return foldKeys(state, foldFunc, acc, mode, hasHintfile(state));
    }

    private static boolean hasHintfile(FileState state) {
        return isFile(hintfileName(state));
    }

    private static boolean isFile(String filename) {
        final File f = new File(filename);
        return f.isFile() || f.isDirectory();
    }

    static <A> A foldKeys(FileState state, RecordFoldFunction<A, KeyRecord> foldFunc, A acc, KeyFoldMode mode, boolean hasHintfile) throws IOException, InterruptedException {
        if (mode == KeyFoldMode.DEFAULT) {
            if (hasHintfile) {
                return foldHintfile(state, foldFunc, acc);
            } else {
                return foldKeysLoop(state, 0, foldFunc, acc);
            }
        }
        if (mode == KeyFoldMode.RECOVERY) {
            if (hasHintfile) {
                return foldKeys(state, foldFunc, acc, KeyFoldMode.RECOVERY, true, hasValidHintfile(state));
            } else {
                return foldKeysLoop(state, 0, foldFunc, acc);
            }
        }
        throw new BitCaskError("foldKeys case unreachable");
    }

    /**
     * Return true if there is a hintfile and it has
     * a valid CRC check
     */
    static boolean hasValidHintfile(FileState state) {
        final String hintFile = hintfileName(state);
        // TODO find how to ask for a READ_AHEAD in JVM
        try {
            final FileChannel hintFD = IO.fileOpen(hintFile, Collections.singletonList(OpenMode.READONLY));
            try {
                final long hintSize = hintFD.size();
                if (hintSize > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("File size must be at most Integer.MAX_VALUE");
                }
                return hintfileValidateLoop(hintFD, 0, (int) hintSize);
            } finally {
                hintFD.close();
            }
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean hintfileValidateLoop(FileChannel hintFD, long crc0, int rem) {
        int readLen;
        boolean hasCRC;
        if (rem <= JBitCask.CHUNK_SIZE) {
            if (rem < HINT_RECORD_SZ) {
                return false;
            } else {
                readLen = rem - HINT_RECORD_SZ;
                hasCRC = true;
            }
        } else {
            readLen = JBitCask.CHUNK_SIZE;
            hasCRC = false;
        }
        final ByteBuffer bytes = ByteBuffer.allocate(readLen);
        try {
            hintFD.read(bytes);
            CRC32 crc = new CRC32();
            crc.update(ByteBuffer.allocate(8).putLong(crc0).flip());
            crc.update(bytes);
            if (hasCRC) {
                long expectCRC = readCrc(hintFD);
                return expectCRC == crc.getValue();
            } else {
                return hintfileValidateLoop(hintFD, crc.getValue(), rem - readLen);
            }
        } catch (IOException e) {
            return false;
        }
    }

    private static long readCrc(FileChannel fd) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(HINT_RECORD_SZ);
        fd.read(data);
        final int tstamp = data.getInt();//TSTAMPFIELD
        final short keySize = data.getShort(); //KEYSIZEFIELD
        final int expectedCRC = data.getInt();//TOTALSIZEFIELD
        final long tombstone = data.getLong(); //TOMBSTONEFIELD_V2 + OFFSETFIELD_V2 = 64
        final long offset = tombstone & JBitCask.MAXOFFSET_V2;
        if (tstamp == 0 && keySize == 0 && offset == JBitCask.MAXOFFSET_V2) {
            return expectedCRC;
        }
        throw new IOException("Bad CRC retrieval");
    }

    static <A> A foldKeys(FileState state, RecordFoldFunction<A, KeyRecord> foldFun, A acc, KeyFoldMode mode,
                          Object dontcare, boolean validHintFile) throws IOException, InterruptedException {
        if (mode != KeyFoldMode.RECOVERY) {
            throw new IllegalArgumentException("mode MUST be RECOVERY");
        }
        if (validHintFile) {
            return foldHintfile(state, foldFun, acc);
        } else {
            String hintFile = hintfileName(state);
            System.err.printf("Hintfile '%s' invalid%n", hintFile);
            return foldKeysLoop(state, 0, foldFun, acc);
        }
    }

    private static <A> A foldKeysLoop(FileState fs, long offset, RecordFoldFunction<A, KeyRecord> fun, A acc) throws IOException, InterruptedException {
        fs.fd.position(offset);
        return foldFileLoop(fs.fd, FileType.REGULAR, FileOperations::foldKeysIntLoop, fun,
                acc, new KeyData(fs.filename, fs.timestamp, offset, 0));
    }

    static class PosInfo {
        private final String filename;
        private final Long fTStamp;
        final long offset;
        final long totalSize;

        public PosInfo(String filename, Long fTStamp, long offset, long totalSize) {
            this.filename = filename;
            this.fTStamp = fTStamp;
            this.offset = offset;
            this.totalSize = totalSize;
        }

        public PosInfo(long offset, int totalSize) {
            this(null, null, offset, totalSize);
        }
    }

    public static <A> A fold(FileState f, RecordFoldFunction<A, KeyValueRecord> fun, A acc) throws IOException, InterruptedException {
        //TODO if f.isFresh() return acc
        //TODO: Add some sort of check that this is a read-only file
        f.fd.position(0L);
        PosInfo posInfo = new PosInfo(f.filename, f.timestamp, 0, 0 );
        return foldFileLoop(f.fd, FileType.REGULAR, FileOperations::foldIntLoop, fun, acc, posInfo);
    }

    private static <A> BytesFoldFunResult<PosInfo, A> foldIntLoop(ByteBuffer data, RecordFoldFunction<A, KeyValueRecord> fun, A acc,
                                                                  long consumed, PosInfo args) throws IOException, InterruptedException {
        // L544
        if (args.totalSize == 20) {
            System.err.printf("fold_loop: CRC error limit at file %s offset %d%n", args.filename, args.offset);
            return new BytesFoldFunResult<>(BytesFoldFunResult.Type.DONE, acc);
        }

        try {
            final int crc32 = data.getInt(); //CRCSIZEFIELD
            final int sliceStart = data.position();
            final int tstamp = data.getInt(); //TSTAMPFIELD
            final int keySize = data.getShort(); //KEYSIZEFIELD
            final int valueSize = data.getInt(); //VALSIZEFIELD

            final byte[] key = new byte[keySize];
            data.get(key);

            final byte[] value = new byte[valueSize];
            data.get(value);

            final int totalSize = keySize + valueSize + JBitCask.HEADER_SIZE;
            final CRC32 crcCodec = new CRC32();
            crcCodec.update(data.slice(sliceStart, JBitCask.TSTAMPFIELD + JBitCask.KEYSIZEFIELD + JBitCask.VALSIZEFIELD));
            crcCodec.update(key);
            crcCodec.update(value);
            final long calcCrc = crcCodec.getValue();

            if (crc32 != calcCrc) {
                System.err.printf("fold_loop: CRC error at file %s offset %d, skipping %d bytes%n", args.filename, args.offset, totalSize);
                return foldIntLoop(data, fun, acc, consumed + totalSize,
                        new PosInfo(args.filename, args.fTStamp, args.offset + totalSize, args.totalSize + 1));
            }

            PosInfo posInfo = new PosInfo(args.filename, args.fTStamp, args.offset, totalSize);
            A accNew = fun.fold(new KeyValueRecord(key, value), tstamp, posInfo, acc);

            return foldIntLoop(data, fun, accNew, consumed + totalSize, new PosInfo(args.filename, args.fTStamp,
                    args.offset + totalSize, args.totalSize));
        } catch (BufferUnderflowException underflow) {
            return new BytesFoldFunResult<>(BytesFoldFunResult.Type.MORE, acc, consumed, new PosInfo(args.filename, args.fTStamp, args.offset, args.totalSize));
        }
    }

    private static <A> BytesFoldFunResult<KeyData, A> foldKeysIntLoop(ByteBuffer data, RecordFoldFunction<A, KeyRecord> fun, A acc, long consumed,
                                                                      KeyData args) throws IOException, InterruptedException {
        if (args.crcSkipCount == 20) {
            System.err.printf("fold_loop: CRC error limit at file %s offset %d%n", args.filename, args.offset);
            return new BytesFoldFunResult<>(BytesFoldFunResult.Type.DONE, acc);
        }
        try {
            final int crc32 = data.getInt(); //CRCSIZEFIELD
            final int sliceStart = data.position();
            final int tstamp = data.getInt(); //TSTAMPFIELD
            final int keySize = data.getShort(); //KEYSIZEFIELD
            final int valueSize = data.getInt(); //VALSIZEFIELD

            final byte[] key = new byte[keySize];
            data.get(key);

            final byte[] value = new byte[valueSize];
            data.get(value);

            final int totalSize = keySize + valueSize + JBitCask.HEADER_SIZE;
            final CRC32 crcCodec = new CRC32();
            crcCodec.update(data.slice(sliceStart, JBitCask.TSTAMPFIELD + JBitCask.KEYSIZEFIELD + JBitCask.VALSIZEFIELD));
            crcCodec.update(key);
            crcCodec.update(value);
            final long calcCrc = crcCodec.getValue();
            if (crc32 != calcCrc) {
                System.err.printf("fold_loop: CRC error at file %s offset %d, skipping %d bytes%n", args.filename, args.offset, totalSize);
                return foldKeysIntLoop(data, fun, acc, consumed + totalSize,
                        new KeyData(args.filename, args.fTStamp, args.offset + totalSize, args.crcSkipCount + 1));
            }
            final boolean tombstone = JBitCask.isTombstone(value);
            A accNew = fun.fold(new KeyRecord(tombstone, key), tstamp, new PosInfo(args.offset, totalSize), acc);
            return foldKeysIntLoop(data, fun, accNew, consumed + totalSize, new KeyData(args.filename, args.fTStamp,
                    args.offset + totalSize, args.crcSkipCount));
        } catch (BufferUnderflowException underflow) {
            return new BytesFoldFunResult<>(BytesFoldFunResult.Type.MORE, acc, consumed, new KeyData(args.filename, args.fTStamp, args.offset, args.crcSkipCount));
        }
    }

    enum FileType {HINT, REGULAR}

    private static <A> A foldHintfile(FileState state, RecordFoldFunction<A, KeyRecord> foldFun, A acc) throws IOException, InterruptedException {
        final String hintFile = hintfileName(state);
        // TODO find how to ask for a READ_AHEAD in JVM
        final FileChannel hintFD = IO.fileOpen(hintFile, Collections.singletonList(OpenMode.READONLY));
        try {
            final long dataSize = hintFD.size();
            return foldFileLoop(hintFD, FileType.HINT, FileOperations::foldHintfileLoop, foldFun, acc, new HintfileData(dataSize, hintFile));
        } finally {
            hintFD.close();
        }
    }

    // main work loop here, containing the full match of hint record and key.
    // if it gets a match, it proceeds to recurse over the rest of the big
    // binary
    // same signature of BytesFoldFunction
    private static <A> BytesFoldFunResult<HintfileData, A> foldHintfileLoop(ByteBuffer data, RecordFoldFunction<A, KeyRecord> fun, A acc, long consumed,
                                                                            HintfileData args) throws IOException, InterruptedException {
        if (data.remaining() < JBitCask.TSTAMPFIELD + JBitCask.KEYSIZEFIELD + JBitCask.TOTALSIZEFIELD +
                JBitCask.TOMBSTONEFIELD_V2 + JBitCask.OFFSETFIELD_V2) {
            //catchall case where we don't get enough bytes from fold_file_loop
            return new BytesFoldFunResult<>(BytesFoldFunResult.Type.MORE, acc, consumed, args);
        }
        final int tstamp = data.getInt();//TSTAMPFIELD
        final short keySize = data.getShort(); //KEYSIZEFIELD
        final int totalSize = data.getInt();//TOTALSIZEFIELD
        final long tombstone = data.getLong(); //TOMBSTONEFIELD_V2 + OFFSETFIELD_V2 = 64
        final long offset = tombstone & JBitCask.MAXOFFSET_V2;
        final long tombInt = tombstone >> 63;

        if (data.remaining() == 0) {
            if (tombstone == 0 && keySize == 0 && offset == JBitCask.MAXOFFSET_V2) {
                return new BytesFoldFunResult<>(BytesFoldFunResult.Type.DONE, acc, consumed + HINT_RECORD_SZ);
            } else {
                return new BytesFoldFunResult<>(BytesFoldFunResult.Type.MORE, acc, consumed, args);
            }
        }

        final byte[] key = new byte[keySize];
        data.get(key);
        if (offset + totalSize > args.dataSize + 1) {
            System.err.printf("Hintfile '%s' contains pointer %d %d that is greater than total data size %d%n",
                    args.hintFile, offset, totalSize, args.dataSize);
            throw new BitCaskError("trunc_hintfile");
        }
        A accNew = fun.fold(new KeyRecord(tombInt > 0, key), tstamp, new PosInfo(offset, totalSize), acc);
        long consumedNew = keySize + HINT_RECORD_SZ + consumed;
        return foldHintfileLoop(data, fun, accNew, consumedNew, args);
    }

    private static <T, A, R> A foldFileLoop(FileChannel fd, FileType type, BytesFoldFunction<T, A, R> foldFun,
                                            RecordFoldFunction<A, R> intFoldFn, A acc, T args) throws IOException, InterruptedException {
        return foldFileLoop(fd, type, foldFun, intFoldFn, acc, args, null, JBitCask.CHUNK_SIZE);
    }

    private static <T, A, R> A foldFileLoop(FileChannel fd, FileType type, BytesFoldFunction<T, A, R> foldFun,
                                            RecordFoldFunction<A, R> intFoldFn, A acc, T args,
                                            ByteBuffer prev, int chunkSize) throws IOException, InterruptedException {
        // analyze what happened in the last loop to determine whether or
        // not to change the read size. This is an optimization for large values
        // in datafile folds and key folds
        final ByteBuffer prevNew;
        final int chunkSizeNew;
        if (prev == null) {
            prevNew = ByteBuffer.allocate(0);
            chunkSizeNew = chunkSize;
        } else {
            if (prev.remaining() >= JBitCask.MAX_CHUNK_SIZE) {
                // to avoid having to rescan the same
                // binaries over and over again.
                chunkSizeNew = JBitCask.MAX_CHUNK_SIZE;
            } else if (prev.remaining() >= chunkSize) {
                chunkSizeNew = chunkSize * 2;
            } else {
                chunkSizeNew = chunkSize;
            }
            prevNew = prev;
        }
        final ByteBuffer bytes = ByteBuffer.allocate(prevNew.remaining() + chunkSizeNew);
        bytes.put(prev);
        try {
            fd.read(bytes);
            bytes.flip();
            int bytesSize = bytes.remaining();
            BytesFoldFunResult<T, A> res = foldFun.fold(bytes, intFoldFn, acc, 0, args);
            if (res.type == BytesFoldFunResult.Type.MORE) {
                // foldfuns should return more when they don't have enough
                // bytes to satisfy their main binary match.
                final ByteBuffer rest;
                if (res.consumed > bytesSize) {
                    rest = null;
                } else {
                    rest = bytes;
                }
                return foldFileLoop(fd, type, foldFun, intFoldFn, res.acc, res.data, rest, chunkSizeNew);
            } else if (res.type == BytesFoldFunResult.Type.DONE && res.containOnlyAcc()) {
                // the done two tuple is returned when we want to be
                // unconditionally successfully finished,
                // i.e. trailing data is a non-fatal error
                return res.acc;
            } else if (res.type == BytesFoldFunResult.Type.DONE) {
                // three tuple done requires full consumption of all
                // bytes given to the internal fold function, to
                // satisfy the pre-existing semantics of hintfile
                // folds.
                if (res.consumed == bytesSize) {
                    return res.acc;
                } else {
                    throw new BitCaskError("partial_fold");
                }
            }
            throw new BitCaskError("Case unreachable");
        } catch (EOFException eof) {
            // when we reach the end of the file, if it's a hintfile,
            // we need to make sure that require_hint_crc is honored
            // (or not as the case may be).
            if (prevNew.remaining() == 0 && type == FileType.HINT) {
                if (Boolean.parseBoolean(System.getenv("require_hint_crc"))) {
                    throw new Error("incomplete_hint, 4");
                } else {
                    return acc;
                }
            } else {
                return acc;
            }
        }
    }

    /**
     * Close a file for writing, but leave it open for reads.
     *
     * @param fileState
     */
    public static FileState closeForWriting(FileState fileState) throws IOException {
        final FileState state = closeHintfile(fileState);
        fileState.fd.force(false);
        return new FileState(OpenMode.READONLY, state.filename, state.timestamp, state.fd, state.ofs,
                state.hintcrc, state.hintfd);
    }

    /**
     * Use when done writing a file.  (never open for writing again)
     */
    public static void close(FileState fileState) throws IOException {
        closeHintfile(fileState);
        fileState.fd.close();
    }

    /**
     * Use when closing multiple files.  (never open for writing again)
     */
    public static void closeAll(List<FileState> fileStates) throws IOException {
        for (FileState state : fileStates) {
            close(state);
        }
    }

    private static FileState closeHintfile(FileState fileState) throws IOException {
        if (fileState.hintfd == null) {
            return fileState;
        }
        // Write out CRC check at end of hint file.  Write with an empty key, zero
        // timestamp and offset as large as the file format supports so opening with
        // an older version of bitcask will just reject the record at the end of the
        // hintfile and otherwise work normally.

        ByteBuffer ioList = hintfileEntry(new byte[0], 0, 0, JBitCask.MAXOFFSET_V2, fileState.hintcrc);
        fileState.hintfd.write(ioList);
        fileState.hintfd.close();
        return new FileState(fileState.mode, fileState.filename, fileState.timestamp, fileState.fd, fileState.ofs);
    }

    private static ByteBuffer hintfileEntry(byte[] key, int timestamp, int tombInt, long offset, long totalSize) {
        final int keySize = key.length;
        final ByteBuffer b = ByteBuffer.allocate(256);
        b.putInt(timestamp);
        b.putShort((short) keySize);
        b.putInt((int) totalSize);

        //TODO merge 1 bit + 63
        b.putLong(JBitCask.TOMBSTONEFIELD_V2);
        b.putInt(JBitCask.OFFSETFIELD_V2);

        b.put(key);
        return b.flip();
    }

    enum WriteResponse {FRESH, WRAP, OK}

    public static WriteResponse checkWrite(FileState fileState, byte[] key, int valSize, long maxSize) {
        if (fileState == null) {
            // for the very first write, special-case
            return WriteResponse.FRESH;
        }
        final int size = JBitCask.HEADER_SIZE + key.length + valSize;
        if (fileState.ofs + size > maxSize) {
            return WriteResponse.WRAP;
        } else {
            return WriteResponse.OK;
        }
    }

    static class WriteResult {
        final FileState fileState;
        final long offset;
        final int size;

        public WriteResult(FileState fileState, long offset, int size) {
            this.fileState = fileState;
            this.offset = offset;
            this.size = size;
        }
    }

    /**
     * Write a Key-named binary data field ("Value") to the Filestate.
     */
    public static WriteResult write(FileState fileState, byte[] key, byte[] value, long timestamp) throws IOException {
        if (fileState.mode == OpenMode.READONLY) {
            throw new BitCaskError("read only");
        }
        if (key.length > JBitCask.MAXKEYSIZE) {
            throw new IllegalArgumentException("key length " + key.length + " exceeded " + JBitCask.MAXKEYSIZE);
        }
        if (value.length > JBitCask.MAXVALSIZE) {
            throw new IllegalArgumentException("value length " + value.length + " exceeded " + JBitCask.MAXVALSIZE);
        }
        final int fieldSize = JBitCask.TSTAMPFIELD + JBitCask.KEYSIZEFIELD + JBitCask.VALSIZEFIELD + key.length + value.length;
        final ByteBuffer field = ByteBuffer.allocate(fieldSize);
        field.putInt((int) timestamp).putInt(key.length).putInt(value.length).put(key).put(value).flip();
        final CRC32 crcCalculator = new CRC32();
        crcCalculator.update(field);
        final long crc = crcCalculator.getValue();
        field.rewind();
        final int totalSize = JBitCask.CRCSIZEFIELD + fieldSize;
        final ByteBuffer crcField = ByteBuffer.allocate(totalSize);
        crcField.putInt((int) crc).put(field).flip();

        //Store the full entry in the data file
        fileState.fd.write(crcField, fileState.ofs);
        final boolean tombstone = JBitCask.isTombstone(value);

        final ByteBuffer hintEntry = hintfileEntry(key, (int) timestamp, tombstone ? 1 : 0, fileState.ofs, totalSize);
        if (fileState.hintfd != null) {
            fileState.hintfd.write(hintEntry);
            hintEntry.flip();
        }

        // Record our final offset
        crcCalculator.reset();
        crcCalculator.update((int) fileState.hintcrc);
        crcCalculator.update(hintEntry);
        hintEntry.flip();
        final int hintCRC = (int) crcCalculator.getValue(); // compute crc of hint
        final FileState newState = new FileState(fileState.mode, fileState.filename, fileState.timestamp, fileState.fd,
                fileState.ofs + totalSize, hintCRC, fileState.hintfd, fileState.ofs, hintEntry.remaining(), hintCRC);
        return new WriteResult(newState, fileState.ofs, totalSize);
    }

    /**
     * WARNING: We can only undo the last write.
     */
    public static FileState unWrite(FileState fileState) throws IOException {
        fileState.fd.truncate(fileState.lastOfs);
        fileState.fd.position(0); // TODO why put the position to the start?
        final long current = fileState.hintfd.position();
        fileState.hintfd.truncate(current - fileState.lastHintBytes);

        return new FileState(fileState.mode, fileState.filename, fileState.timestamp, fileState.fd, fileState.lastOfs,
                fileState.lastHintCrc, fileState.hintfd, fileState.lastOfs, fileState.lastHintBytes,
                fileState.lastHintCrc);
    }

    static class KeyValue {
        final byte[] key;
        final byte[] value;

        public KeyValue(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Given an Offset and Size, get the corresponding k/v from Filename.
     */
    public static KeyValue read(String filename, long offset, int size) throws IOException {
        final FileState fileState = openFile(Path.of(filename));
        return read(fileState, offset, size);
    }

    public static KeyValue read(FileState fileState, long offset, int size) throws IOException {
        final ByteBuffer data = ByteBuffer.allocate(size);
        fileState.fd.read(data, offset);
        final int crc32 = data.getInt();
        data.mark();
        // Verify the CRC of the data
        final CRC32 crcCodec = new CRC32();
        crcCodec.update(data);
        if (crc32 != crcCodec.getValue()) {
            throw new BadCrcError();
        }
        // Unpack the actual data
        data.reset();
        final int timestamp = data.getInt();
        final short keySize = data.getShort();
        final int valueSize = data.getInt();
        byte[] key = new byte[keySize];
        byte[] value = new byte[valueSize];
        data.get(key);
        data.get(value);
        return new KeyValue(key, value);
    }
}