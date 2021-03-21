package org.dna.jbitcask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dna.jbitcask.FunctionResult.Atom;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class Keydir {

    private static final Logger LOG = LogManager.getLogger(Keydir.class);

    static class BitcaskPrivData {
        Lock globalKeydirsLock = new ReentrantLock();
        Map<String, Keydir> globalKeydirs = new HashMap<>();
        Map<String, Integer> globalBiggestFileId = new HashMap<>();
    }

    // Holds values fetched from a regular entry or a snapshot from an entry list.
    static class EntryProxy {
        int fileId;
        int totalSize;
        long epoch;
        long offset;
        long tstamp; //seonds
        boolean isTombstone;
        byte[] key;

        public EntryProxy(KeydirEntry keydirEntry) {
            this.fileId = keydirEntry.fileId;
            this.totalSize = keydirEntry.totalSize;
            this.epoch = keydirEntry.epoch;
            this.offset = keydirEntry.offset;
            this.tstamp = keydirEntry.tstamp;
            this.key = keydirEntry.key;
        }
    }

    private final class ListKeydirEntry extends ArrayList<KeydirEntry> implements AbstractKeydirEntry {
        @Override
        public boolean isList() {
            return true;
        }
    }

    private final class KeydirEntry implements AbstractKeydirEntry {
        int fileId;
        int totalSize;
        long offset;
        long epoch;
        long tstamp;
        byte[] key;

        public KeydirEntry() {}

        public KeydirEntry(EntryProxy copy) {
            this.fileId = copy.fileId;
            this.totalSize = copy.totalSize;
            this.epoch = copy.epoch;
            this.offset = copy.offset;
            this.tstamp = copy.tstamp;
            this.key = copy.key;
        }

        // Related to tombstones in the pending hash.
        // Notice that tombstones in the entries hash are different.
        boolean isPendingTombstone() {
            return offset == MAX_OFFSET;
        }

        void setPendingTombstone() {
            offset = MAX_OFFSET;
        }

        @Override
        public boolean isList() {
            return false;
        }
    }

    private interface AbstractKeydirEntry {
        boolean isList();
    }

    class FstatsEntry {

        public int fileId;
        public int liveKeys; // number of 'live' keys in entries and pending
        public int liveBytes; // number of 'live' bytes
        public int totalKeys; // total number of keys written to file
        public int totalBytes; // total number of bytes written to file
        public long oldestTstamp; // oldest observed tstamp in a file
        public long newestTstamp; // newest observed tstamp in a file
        public long expirationEpoch; // file obsolete at this epoch
    }

//    uint64_t      sweep_last_generation; // iter_generation of last sibling sweep
//    khiter_t      sweep_itr;             // iterator for sibling sweep
//    uint64_t      pending_updated;
//    uint64_t      pending_start_time;  // UNIX epoch seconds (since 1970)
//    uint64_t      pending_start_epoch;
//    ErlNifPid*    pending_awaken; // processes to wake once pending merged into entries
//    unsigned int  pending_awaken_count;
//    unsigned int  pending_awaken_size;
//    ErlNifMutex*  mutex;
//    char          is_ready;
//    char          name[0];

    private final String name;

    // The hash where entries are usually stored. It may contain
    // regular entries or entry lists created during keyfolding.
    private final Map<byte[], AbstractKeydirEntry> entries = new HashMap<>();
    // Hash used when it's not possible to update entries without
    // resizing it, which would break ongoing keyfolder on it.
    // It can only contain regular entries, not entry lists.
    private Map<byte[], KeydirEntry> pending;
    private final Map<Integer, FstatsEntry> fstats = new HashMap<>();
    private boolean iterating = false;
    private long epoch;
    private long keyCount;
    private long keyBytes;
    private long biggestFileId;
    private int refcount = 1;
    private int keyfolders = 0;
    private long newestFolder;  // Epoch for newest folder
    private long iterGeneration;
    private short iterMutation; // Mutation while iterating?
    private long sweepLastGeneration; // iter_generation of last sibling sweep
    private Iterator<Map.Entry<byte[], AbstractKeydirEntry>> sweepIter = entries.entrySet().iterator(); // iterator for sibling sweep entries.
    private long pendingUpdated;
    private long pendingStartTime; // UNIX epoch seconds (since 1970)
    private long pendingStartEpoch;

    private Lock mutex;
    private boolean ready = false;

    final static BitcaskPrivData PRIV = new BitcaskPrivData();

    private Keydir() {
        this.name = "merge delete keydir";
    }

    private Keydir(String dirname) {
        this.name = dirname;
    }

    private void setBiggestFileId(int biggestFileId) {
        this.biggestFileId = biggestFileId;
    }

    private void incRefCount() {
        refcount++;
    }

    private boolean isReady() {
        return this.ready;
    }

    public void markReady() {
        mutex.lock();
        this.ready = true;
        mutex.unlock();
    }

    static FunctionResult<Atom, Keydir> create(String dirname) {
        // Get our private stash and check the global hash table for this entry
        PRIV.globalKeydirsLock.lock();
        Keydir keydir = PRIV.globalKeydirs.get(dirname);
        if (keydir != null) {
            // Existing keydir is available. Check the is_ready flag to determine if
            // the original creator is ready for other processes to use it.
            if (!keydir.isReady()) {
                // Notify the caller that while the requested keydir exists, it's not
                // ready for public usage.
                PRIV.globalKeydirsLock.unlock();
                return new FunctionResult<>(Atom.NOT_READY, null);
            } else {
                keydir.incRefCount();
            }
        } else {
            // No such keydir, create a new one and add to the globals list. Make sure
            // to allocate enough room for the name.
            keydir = new Keydir(dirname);
            keydir.mutex = new ReentrantLock();
            // Finally, register this new keydir in the globals
            PRIV.globalKeydirs.put(dirname, keydir);
            final Integer biggestFileId = PRIV.globalBiggestFileId.get(dirname);
            if (biggestFileId != null) {
                keydir.setBiggestFileId(biggestFileId);
            }
        }
        PRIV.globalKeydirsLock.unlock();

        // Return to the caller a tuple with the reference and an atom
        // indicating if the keydir is ready or not.
        return new FunctionResult<>(keydir.isReady() ? Atom.READY : Atom.NOT_READY, keydir);
    }

    /**
     * @return the new fileId
     * */
    public long incrementFileId() {
        return incrementFileId(0L);
    }

    public long incrementFileId(Long conditionalFileId) {
        mutex.lock();
        if (conditionalFileId == null || conditionalFileId == 0) {
            biggestFileId++;
        } else {
            if (conditionalFileId > biggestFileId) {
                biggestFileId = conditionalFileId;
            }
        }
        long newId = biggestFileId;
        mutex.unlock();
        return newId;
    }

    public void keydirRemove(byte[] key) {
        keydirRemove(key, System.currentTimeMillis());
    }
//TODO revisit in case is conditional is true
    public void keydirRemove(byte[] key, long timestampMillis) {
        int removeTime = 0;
        mutex.lock();
        this.epoch += 1;
        LOG.debug("+++ Remove");
        perhapsSweepSiblings();
        final FindResult fr = findKeydirEntry(key, this.epoch);
        if (!fr.found || fr.proxy.isTombstone) {
            // not found
            mutex.unlock();
            return;
        }
        dokeydirRemove(key, removeTime, fr);
        mutex.unlock();
    }

    // This is a conditional removal. We
    // only want to actually remove the entry if the tstamp, fileid and
    // offset matches the one provided. A sort of poor-man's CAS.
    /**
     * @throws AlreadyExistsError
     * */
    public void keydirRemove(byte[] key, long timestamp, int fileId, long offset) {
        int removeTime = 0;
        mutex.lock();
        this.epoch += 1;
        LOG.debug("+++ Remove conditional");
        perhapsSweepSiblings();
        final FindResult fr = findKeydirEntry(key, this.epoch);
        if (!fr.found || fr.proxy.isTombstone) {
            // not found
            mutex.unlock();
            return;
        }
        // conditional remove, bail if not a match.
        if (fr.proxy.tstamp != timestamp ||
                fr.proxy.fileId != fileId ||
                fr.proxy.offset != offset) {
            mutex.unlock();
            LOG.debug("+++Conditional no match");
            throw new AlreadyExistsError();
        }
        dokeydirRemove(key, removeTime, fr);
        mutex.unlock();
    }

    private void dokeydirRemove(byte[] key, int removeTime, FindResult fr) {
        // Remove the key from the keydir stats
        keyCount --;
        keyBytes -= fr.proxy.key.length;
        if (keyfolders > 0) {
            iterMutation = 1;
        }

        // Remove from file stats
        updateFstats(fr.proxy.fileId, fr.proxy.tstamp, MAX_EPOCH, -1, 0, -fr.proxy.totalSize,
                0, false);

        // If found an entry in the pending hash, convert it to a tombstone
        if (fr.pendingEntry != null) {
            System.out.println("LINE 201 pending put");
            fr.pendingEntry.setPendingTombstone();
            fr.pendingEntry.tstamp = removeTime;
            fr.pendingEntry.epoch = this.epoch;
        } else if (this.pending != null) {
            // If frozen, add tombstone to pending hash (iteration must have
            // started between put/remove call in bitcask:delete.
            final KeydirEntry pendingEntry = new KeydirEntry(fr.proxy);
            pendingEntry.setPendingTombstone();
            pendingEntry.tstamp = removeTime;
            pendingEntry.epoch = this.epoch;
            pending.put(key, pendingEntry);
        } else if(this.keyfolders == 0) {
            // If not iterating, just remove.
            this.entries.remove(key);
        } else {
            // else found in entries while iterating
            setEntryTombstone(key, fr.entriesEntry, removeTime, this.epoch);
        }
        LOG.debug("Removed");
    }

    // Adds a tombstone to an existing entries hash entry. Regular entries are
    // converted to lists first. Only to be called during iterations.
    // Entries are simply removed when there are no iterations.
    private void setEntryTombstone(byte[] key, AbstractKeydirEntry entry, int removeTime, long removeEpoch) {
        KeydirEntry tombstone = new KeydirEntry();
        tombstone.tstamp = removeTime;
        tombstone.epoch = removeEpoch;
        tombstone.offset = MAX_OFFSET;
        tombstone.totalSize = MAX_SIZE;
        tombstone.fileId = MAX_FILE_ID;

        if (!entry.isList()) {
            // update into an entry list
            final ListKeydirEntry newEntryList = new ListKeydirEntry();
            newEntryList.add(tombstone);
            entries.put(key, newEntryList);
        } else {
            //need to update the entry list with a tombstone
            updateEntryList((ListKeydirEntry) entry, tombstone, keyfolders > 0);
        }
    }

    void updateFstats(int fileId, long tstamp, long expirationEpoch, int liveIncrement,
                              int totalIncrement, int liveBytesIncrement, int totalBytesIncrement, boolean shouldCreate) {
        FstatsEntry entry = fstats.get(fileId);
        if (entry == null) {
            if (!shouldCreate) {
                return;
            }
            // Need to initialize new entry and add to the table
            entry = new FstatsEntry();
            entry.expirationEpoch = MAX_EPOCH;
            entry.fileId = fileId;
            fstats.put(fileId, entry);
        }

        entry.liveKeys += liveIncrement;
        entry.totalKeys += totalIncrement;
        entry.liveBytes += liveBytesIncrement;
        entry.totalBytes += totalBytesIncrement;

        if (expirationEpoch < entry.expirationEpoch) {
            entry.expirationEpoch = expirationEpoch;
        }

        if ((tstamp != 0 && tstamp < entry.oldestTstamp) || entry.oldestTstamp == 0) {
            entry.oldestTstamp = tstamp;
        }

        if ((tstamp != 0 && tstamp > entry.newestTstamp) || entry.newestTstamp == 0) {
            entry.newestTstamp = tstamp;
        }
    }

    // All info about a lookup with find_keydir_entry.
    private class FindResult {
        // Entry found in the pending hash. If set, entries_entry will be NULL.
        KeydirEntry pendingEntry;
        // Entry found in the entries hash. If set, pending_entry is NULL
        AbstractKeydirEntry entriesEntry;
        // Copy of the values of the found entry, if any, whether it's
        // a regular entry or list.
        EntryProxy proxy;
        // Hash (entries or pending) where the entry was found.
        Map hash;

        // True if found, even if it is a tombstone
        boolean found;
    }

    // Find an entry in the pending hash when they keydir is frozen, or in the
    // entries hash otherwise.
    private FindResult findKeydirEntry(byte[] key, long epoch) {
        // Search pending. If keydir handle used is in iterating mode
        // we want to see a past snapshot instead.
        final FindResult ret = new FindResult();
        if (pending != null) {

            ret.pendingEntry = pending.get(key);
            if (ret.pendingEntry != null && epoch >= ret.pendingEntry.epoch) {
                System.out.printf("Found in pending %d > %d%n", epoch, ret.pendingEntry.epoch);
                ret.hash = this.pending;
                ret.entriesEntry = null;
                ret.found = true;
                ret.proxy = findProxy(ret.pendingEntry);
                return ret;
            }
        }
        // Definitely not in the pending entries
        ret.pendingEntry = null;

        // If a snapshot for that time is found in regular entries
        final AbstractKeydirEntry entryProxies = entries.get(key);
        ret.entriesEntry = entryProxies;
        ret.proxy = findProxyAtEpoch(entryProxies, epoch);
        if (entryProxies != null && ret.proxy != null) {
            ret.hash = this.entries;
            ret.found = true;
            return ret;
        }
        ret.entriesEntry = null;
        ret.hash = null;
        ret.found = false;
        return ret;
    }

    private void perhapsSweepSiblings() {
        int maxMicrosec = 600;
        if (keyfolders > 0 || iterMutation == 0 || sweepLastGeneration == iterGeneration) {
            return;
        }
        LocalTime target = LocalTime.now().plus(maxMicrosec, ChronoUnit.MICROS);
        int i = 0;
        while (i-- != 0) {
            if ((i % 500) == 0) {
                if (LocalTime.now().isAfter(target)) {
                    break;
                }
            }
            if (!sweepIter.hasNext()) {
                sweepIter = entries.entrySet().iterator();
                sweepLastGeneration = iterGeneration;
                return;
            }
            Map.Entry<byte[], AbstractKeydirEntry> val = sweepIter.next();
            if (val.getValue() != null && (val.getValue() instanceof ListKeydirEntry)) {
                ListKeydirEntry listVal = (ListKeydirEntry) val.getValue();
                if (listVal.size() > 1) {
                    EntryProxy proxy = findProxy(listVal);
                    if (proxy != null) {
                        if (proxy.isTombstone) {
                            sweepIter.remove();
                        } else {
                            updateEntry(val.getKey(), proxy);
                        }
                    }
                }
            }
        }
    }

    // Updates an entry from the entries hash, not from pending.
    // Use updateRegularEntry on pending hash entries instead.
    // While iterating, regular entries will become entry lists,
    // otherwise the result is a regular, single value entry.
    private void updateEntry(byte[] key, EntryProxy newValue) {
        final AbstractKeydirEntry currentEntry = entries.get(key);
        final boolean iterating = keyfolders > 0;
        if (iterating) {
            if (currentEntry.isList()) {
                // Add to list of values during iteration
                updateEntryList((ListKeydirEntry) currentEntry, newValue, iterating);
            } else {
                // Convert regular entry to list during iteration
                final ListKeydirEntry newList = new ListKeydirEntry();
                newList.add(new KeydirEntry(newValue));
                newList.add((KeydirEntry) currentEntry);
                entries.put(key, newList);
            }
        } else { // not iterating, so end up with regular entries only.
            if (currentEntry.isList()) {
                // Convert list to regular entry
                entries.put(key, new KeydirEntry(newValue));
            } else {
                // regular entry, no iteration
                entries.put(key, new KeydirEntry(newValue));
            }
        }
    }

    private void updateEntryList(ListKeydirEntry currentEntry, EntryProxy newValue, boolean iterating) {
        if (!iterating) {
            // replace the entries list
            currentEntry.clear();
            currentEntry.add(new KeydirEntry(newValue));
        } else {
            // otherwise put a new head
            currentEntry.add(0, new KeydirEntry(newValue));
        }
    }

    private void updateEntryList(ListKeydirEntry currentEntry, KeydirEntry newValue, boolean iterating) {
        if (!iterating) {
            // replace the entries list
            currentEntry.clear();
            currentEntry.add(newValue);
        } else {
            // otherwise put a new head
            currentEntry.add(0, newValue);
        }
    }

    private static long MAX_EPOCH = Long.MAX_VALUE;
    private static long MAX_OFFSET = Long.MAX_VALUE;
    private static long MAX_TIME = Integer.MAX_VALUE;
    private static int MAX_SIZE = Integer.MAX_VALUE;
    private static int MAX_FILE_ID = Integer.MAX_VALUE;

    // Extracts entry values from a regular entry or the latest snapshot
    // from an entry list.
    private EntryProxy findProxy(ListKeydirEntry old) {
        return findProxyAtEpoch(old, MAX_EPOCH);
    }
    private EntryProxy findProxy(KeydirEntry old) {
        return findProxyAtEpoch(old, MAX_EPOCH);
    }

    // Extracts the entry values from a regular entry or from the
    // closest snapshot in time in an entry list.
    private EntryProxy findProxyAtEpoch(AbstractKeydirEntry old, long epoch) {
        if (old instanceof ListKeydirEntry) {
            return findProxyAtEpoch((ListKeydirEntry) old, epoch);
        }
        return findProxyAtEpoch((KeydirEntry) old, epoch);
    }

    private EntryProxy findProxyAtEpoch(KeydirEntry old, long epoch) {
        if (epoch < old.epoch) {
            return null;
        }
        final EntryProxy ret = new EntryProxy(old);
        ret.isTombstone = old.offset == MAX_OFFSET;
        return ret;
    }

    private EntryProxy findProxyAtEpoch(ListKeydirEntry old, long epoch) {
        KeydirEntry s = null;
        for (KeydirEntry scan : old) {
            if (epoch >= scan.epoch) {
                s = scan;
                break;
            }
        }
        if (s == null) {
            return null;
        }

        final EntryProxy ret = new EntryProxy(s);
        ret.isTombstone = s.fileId == MAX_TIME && s.totalSize == MAX_SIZE && s.offset == MAX_OFFSET;;
        return ret;
    }

    /**
     * @throws AlreadyExistsError
     * */
    public void keydirPut(byte[] key, int fileId, int totalSize, long offset, long timestamp, long nowSeconds,
                          boolean newestPut) {
        keydirPut(key, fileId, totalSize, offset, timestamp, nowSeconds, newestPut, null, null);
    }

    /**
     * @throws AlreadyExistsError
     * */
    public void keydirPut(byte[] key, int fileId, int totalSize, long offset, long timestamp, long nowSeconds,
                          boolean newestPut, Integer oldFileId, Integer oldOffset) throws AlreadyExistsError {
        if (oldFileId != null && oldOffset == null) {
            throw new IllegalArgumentException("When oldFileId is provided also oldOffset must be present");
        }
        KeydirEntry entry = new KeydirEntry();
        entry.fileId = fileId;
        entry.totalSize = totalSize;
        entry.offset = offset;
        entry.key = key;

        mutex.lock();

        LOG.debug("+++ Put key = {} file_id={} offset={} total_sz={} tstamp={}",
                key, entry.fileId, (int) entry.offset, entry.totalSize, entry.tstamp);
        perhapsSweepSiblings();

        final FindResult f = findKeydirEntry(key, MAX_EPOCH);

        // If conditional put and not found, bail early
        if ((!f.found || f.proxy.isTombstone) && oldFileId != null) {
            LOG.debug("file already exists");
            mutex.unlock();
            throw new AlreadyExistsError();
        }

        this.epoch += 1; //don't worry about backing this out if we bail
        entry.epoch = this.epoch;

        // If put would iterating, start pending hash
        if (keyfolders > 0 && pending == null) {
            pending = new HashMap<>();
            pendingStartEpoch = epoch;
            pendingStartTime = nowSeconds;
        }

        if (!f.found || f.proxy.isTombstone) {
            if ((newestPut && (entry.fileId < biggestFileId)) || oldFileId != null) {
                mutex.unlock();
                // already exists
                throw new AlreadyExistsError();
            }
            keyCount ++;
            keyBytes += key.length;
            if (keyfolders > 0) {
                iterMutation = 1;
            }
            // Increment live and total stats.
            updateFstats(entry.fileId, entry.tstamp, MAX_EPOCH, 1, 1, entry.totalSize, entry.totalSize, true);

            putEntry(key, f, entry);
            LOG.debug("+++ Put new, !found || !tombstone");
            mutex.unlock();
            return;
        }

        // Putting only if replacing this file/offset entry, fail otherwise.
        // This is an important part of our optimistic concurrency mechanisms
        // to resolve races between writers (main and merge currently).
        if (oldFileId != null) {
            // This line is tricky: We are trying to detect a merge putting
            // a value that replaces another value that same merge just put
            // (so same output file).  Because when it does that, it has
            // replaced a previous value with smaller file/offset.  It then
            // found yet another value that is also current and should
            // be written to the merge file, but since it has smaller file/ofs
            // than the newly merged value (in a new merge file), it is
            // ignored. This happens with values from the same second,
            // since the out of date logic in merge uses timestamps.
            if ((newestPut || entry.fileId != f.proxy.fileId) &&
                    !(oldFileId == f.proxy.fileId && oldOffset == f.proxy.offset)) {
                LOG.debug("++ Conditional not match");
                mutex.unlock();
                return;
            }
        }

        // Avoid updating with stale data. Allow if:
        // - If real put to current write file, not a stale one
        // - If internal put (from merge, etc) with newer timestamp
        // - If internal put with a higher file id or higher offset
        if ((newestPut &&
                (entry.fileId >= biggestFileId)) ||
                (! newestPut && (f.proxy.tstamp < entry.tstamp)) ||
                (! newestPut && ((f.proxy.fileId < entry.fileId) ||
                                (((f.proxy.fileId == entry.fileId) &&
                                        (f.proxy.offset < entry.offset))))))
        {
            if (keyfolders > 0) {
                iterMutation = 1;
            }
            // Remove the stats for the old entry and add the new
            if (f.proxy.fileId != entry.fileId) {
                // different files
                updateFstats(f.proxy.fileId, 0, MAX_EPOCH, -1, 0,
                        -f.proxy.totalSize, 0, false);
                updateFstats(entry.fileId, entry.tstamp, MAX_EPOCH, 1, 1,
                        entry.totalSize, entry.totalSize, true);
            } else {
                // file_id is same, change live/total in one entry
                updateFstats(entry.fileId, entry.tstamp,
                        MAX_EPOCH, 0, 1,
                        entry.totalSize - f.proxy.totalSize,
                        entry.totalSize, true);
            }
            putEntry(key, f, entry);
            mutex.unlock();
            LOG.debug("Finished put");
            return;
        } else {
            // If not live yet, live stats are not updated, but total stats are
            if (!isReady()) {
                updateFstats(entry.fileId, entry.tstamp, MAX_EPOCH, 0, 1, 0,
                        entry.totalSize, true);
            }
            mutex.unlock();
            LOG.debug("No update");
            throw new AlreadyExistsError();
        }
    }

    // Adds or updates an entry in the pending hash if they keydir is frozen
    // or in the entries hash otherwise.
    private void putEntry(byte[] key, FindResult r, KeydirEntry entry) {
        if (r.pendingEntry != null) {
            // found in pending (keydir is frozen), update that one
            updateRegularEntry(r.pendingEntry, entry);
        } else if (pending != null) {
            // iterating (frozen) and not found in pending, add to pending
            pending.put(key, entry);
            pendingUpdated ++;
        } else if (r.entriesEntry != null) {
            // found in entries, update that one
            updateEntry(key, new EntryProxy(entry));
        } else {
            // Not found and not frozen, add to entries
            entries.put(key, entry);
        }

        if (entry.fileId > biggestFileId) {
            biggestFileId = entry.fileId;
        }
    }

    private void updateRegularEntry(KeydirEntry current, KeydirEntry update) {
        current.fileId = update.fileId;
        current.totalSize = update.totalSize;
        current.epoch = update.epoch;
        current.offset = update.offset;
        current.tstamp = update.tstamp;
    }

    public void release() {
        // If the keydir has a lock, we need to decrement the refcount and
        // potentially release it
        if (mutex != null) {
            PRIV.globalKeydirsLock.lock();

            // Remember biggest_file_id in case someone re-opens the same name
            int globalBiggest = 0, theBiggest = 0;
            if (PRIV.globalBiggestFileId.containsKey(name)) {
                globalBiggest = PRIV.globalBiggestFileId.get(name);
            }
            theBiggest = (int) ((globalBiggest > this.biggestFileId) ? globalBiggest : this.biggestFileId);
            theBiggest++;

            PRIV.globalBiggestFileId.put(name, theBiggest);
            refcount--;
            if (refcount == 0) {
                // This is the last reference to the named keydir. As such,
                // remove it from the hashtable so no one else tries to use it
                PRIV.globalKeydirs.remove(name);
            } else {
                // At least one other reference; just throw away our keydir pointer
                // so the check below doesn't release the memory.
            }
            // Unlock ASAP. Wanted to avoid holding this mutex while we clean up the
            // keydir, since it may take a while to walk a large keydir and free each
            // entry.
            PRIV.globalKeydirsLock.unlock();
        }
    }

    /**
     * @throws KeyNotFoundError
     * */
    public EntryProxy get(byte[] key) throws KeyNotFoundError {
        return get(key, 0xffff_ffff_ffff_ffffL);
    }

    /**
     * @throws KeyNotFoundError
     * */
    public EntryProxy get(byte[] key, long epoch) throws KeyNotFoundError {
        mutex.lock();
        perhapsSweepSiblings();
        final FindResult f = findKeydirEntry(key, epoch);
        if (f.found && !f.proxy.isTombstone) {
            LOG.debug(" ... returned value file id=%d size=%d ofs=%d tstamp=%d tomb=%s",
                    f.proxy.fileId, f.proxy.totalSize, f.proxy.offset, f.proxy.tstamp, f.proxy.isTombstone);
            mutex.unlock();
            return f.proxy;
        } else {
            LOG.debug(" ... not found");
            mutex.unlock();
            throw new KeyNotFoundError();
        }
    }

    /**
     * @throws NotReadyError
     * */
    static Keydir maybeKeydirNew(String dirname) {
        Keydir.PRIV.globalKeydirsLock.lock();
        final Keydir keydir = Keydir.PRIV.globalKeydirs.get(dirname);
        Keydir.PRIV.globalKeydirsLock.unlock();
        if (keydir == null) {
            throw new NotReadyError();
        }

        return keydirNew(dirname);
    }

    static Keydir keydirNew() {
        return new Keydir();
    }

    static Keydir keydirNew(String dirname) {
        // Get our private stash and check the global hash table for this entry
        Keydir.PRIV.globalKeydirsLock.lock();
        Keydir keydir = Keydir.PRIV.globalKeydirs.get(dirname);
        if (keydir != null) {
            // Existing keydir is available. Check the is_ready flag to determine if
            // the original creator is ready for other processes to use it.
            if (!keydir.isReady()) {
                // Notify the caller that while the requested keydir exists, it's not
                // ready for public usage.
                Keydir.PRIV.globalKeydirsLock.unlock();
                throw new NotReadyError();
            } else {
                keydir.incRefCount();
            }
        } else {
            // No such keydir, create a new one and add to the globals list.
            keydir = new Keydir(dirname);
            keydir.mutex = new ReentrantLock();

            // Finally, register this new keydir in the globals
            Keydir.PRIV.globalKeydirs.put(dirname, keydir);

            final Integer oldBiggestFileId = Keydir.PRIV.globalBiggestFileId.get(dirname);
            if (oldBiggestFileId != null) {
                keydir.biggestFileId = oldBiggestFileId;
            }
        }
        Keydir.PRIV.globalKeydirsLock.unlock();

        if (!keydir.isReady()) {
            throw new NotReadyError();
        }

        return keydir;
    }

    public long getIterGeneration() {
        return iterGeneration;
    }

    public void setPendingDelete(long fileId) {
        mutex.lock();
        updateFstats((int) fileId, 0, epoch, 0, 0, 0, 0, false);
        mutex.unlock();
    }
}

