package ru.unlocker.append.only.store.file;

import ru.unlocker.append.only.store.GuidObjectKey;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import ru.unlocker.append.only.store.AppendOnlyStoreException;

/**
 * Index file
 *
 * @author unlocker
 */
public class IndexFile {

    /**
     * index file path
     */
    private final Path path;

    /**
     * index cache
     */
    private Map<GuidObjectKey, IndexEntry> cache;

    /**
     * index file
     *
     * @param path file path
     */
    public IndexFile(Path path) {
        this.path = path;
    }

    /**
     * @return initialization status
     */
    private boolean isInitiated() {
        return cache != null;
    }

    /**
     * initialization
     *
     * @throws AppendOnlyStoreException initialization error
     */
    private void init() throws AppendOnlyStoreException {
        if (isInitiated()) {
            return;
        }
        cache = new HashMap<>(128);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (ObjectInputStream input = new ObjectInputStream(
                Files.newInputStream(path, new OpenOption[]{StandardOpenOption.READ}))) {
            Object obj;
            while ((obj = input.readObject()) != null) {
                if (obj instanceof IndexEntry) {
                    IndexEntry entry = (IndexEntry) obj;
                    this.cache.put(entry.key, entry);
                }
            }
        } catch (IOException | ClassNotFoundException ex) {
            throw new AppendOnlyStoreException(ex);
        }
    }

    /**
     * retrieves index
     *
     * @param key
     * @return
     * @throws AppendOnlyStoreException
     */
    public IndexEntry getIndex(GuidObjectKey key) throws AppendOnlyStoreException {
        if (!isInitiated()) {
            init();
        }
        return cache.get(key);
    }

    /**
     * deletes index
     *
     * @param key
     * @throws AppendOnlyStoreException
     */
    public void deleteIndex(GuidObjectKey key) throws AppendOnlyStoreException {
        if (!isInitiated()) {
            init();
        }
        final IndexEntry entry = cache.get(key);
        if (entry == null || entry.deleted) {
            return;
        }
        try (ObjectOutputStream output = new ObjectOutputStream(
                Files.newOutputStream(path,
                        new OpenOption[]{StandardOpenOption.CREATE_NEW}))) {
            entry.deleted = true;
            for (IndexEntry idx : cache.values()) {
                output.writeObject(idx);
            }
        } catch (IOException ex) {
            throw new AppendOnlyStoreException(ex);
        }
    }

    /**
     * writes new index
     *
     * @param index
     * @throws AppendOnlyStoreException
     */
    public void putIndex(IndexEntry index) throws AppendOnlyStoreException {
        if (!isInitiated()) {
            init();
        }
        if (cache.get(index.key) != null) {
            throw new AppendOnlyStoreException("The is duplicated");
        }
        try (ObjectOutputStream output = new ObjectOutputStream(
                Files.newOutputStream(path,
                        new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.APPEND}))) {
            output.writeObject(index);
            cache.put(index.key, index);
        } catch (IOException ex) {
            throw new AppendOnlyStoreException(ex);
        }
    }

    /**
     * @return key set
     * @throws AppendOnlyStoreException
     */
    public Set<GuidObjectKey> getKeys() throws AppendOnlyStoreException {
        if (!isInitiated()) {
            init();
        }
        return cache.keySet();
    }
}
