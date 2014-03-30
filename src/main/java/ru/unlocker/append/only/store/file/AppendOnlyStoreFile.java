package ru.unlocker.append.only.store.file;

import java.io.File;
import ru.unlocker.append.only.store.GuidObjectKey;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.Set;
import ru.unlocker.append.only.store.AppendOnlyStore;
import ru.unlocker.append.only.store.AppendOnlyStoreException;

/**
 * Storage implementation based on files
 *
 * @author unlocker
 */
public class AppendOnlyStoreFile implements AppendOnlyStore {

    /**
     * name of the data file
     */
    public static final String DATA_FILE = "store.dat";

    /**
     * name of the index file
     */
    public static final String INDEX_FILE = "store.idx";

    /**
     * data file
     */
    private DataFile data;

    /**
     * index file
     */
    private IndexFile index;

    /**
     * Storage implementation based on files
     *
     * @param path directory to place the storage
     * @throws AppendOnlyStoreException path not presented a directory
     */
    public AppendOnlyStoreFile(String path) throws AppendOnlyStoreException {
        File dir = new File(path);
        if (!dir.isDirectory()) {
            throw new AppendOnlyStoreException("The directory is required.");
        }
        this.data = new DataFile(Paths.get(path, DATA_FILE));
        this.index = new IndexFile(Paths.get(path, INDEX_FILE));
    }

    @Override
    public void putObject(GuidObjectKey key, ReadableByteChannel input) throws AppendOnlyStoreException {
        if (index.getKeys().contains(key)) {
            throw new AppendOnlyStoreException("Key is duplicated");
        }
        IndexEntry entry = data.putObject(key, input);
        index.putIndex(entry);
    }

    @Override
    public GuidObjectKey generateKey() {
        return GuidObjectKey.createNew();
    }

    @Override
    public ReadableByteChannel getObject(GuidObjectKey key) throws AppendOnlyStoreException {
        IndexEntry entry = index.getIndex(key);
        if (entry == null || entry.deleted) {
            throw new AppendOnlyStoreException("Requested object is unavailable");
        }
        return data.getObject(entry);
    }

    @Override
    public void deleteObject(GuidObjectKey key) throws AppendOnlyStoreException {
        IndexEntry entry = index.getIndex(key);
        if (!(entry == null || entry.deleted)) {
            index.deleteIndex(key);
            data.deleteObject(entry);
        }
    }

    @Override
    public Set<GuidObjectKey> getKeys() throws AppendOnlyStoreException {
        return index.getKeys();
    }
}
