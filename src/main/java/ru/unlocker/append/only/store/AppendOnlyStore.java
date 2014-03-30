package ru.unlocker.append.only.store;

import java.nio.channels.ReadableByteChannel;
import java.util.Set;

/**
 * Storage
 *
 * @author unlocker
 */
public interface AppendOnlyStore {

    /**
     * writes the object to storage
     *
     * @param key 
     * @param input channel to write
     * @throws AppendOnlyStoreException writing object exception
     */
    void putObject(GuidObjectKey key, ReadableByteChannel input) throws AppendOnlyStoreException;

    /**
     * @return generates unique key
     */
    GuidObjectKey generateKey();

    /**
     * retrieves the object
     *
     * @param key
     * @return object's data
     * @throws AppendOnlyStoreException object reading exception
     */
    ReadableByteChannel getObject(GuidObjectKey key) throws AppendOnlyStoreException;

    /**
     * deletes the object
     *
     * @param key
     * @throws AppendOnlyStoreException deleting exception
     */
    void deleteObject(GuidObjectKey key) throws AppendOnlyStoreException;

    /**
     * @return the set of available keys
     * @throws AppendOnlyStoreException reading exception
     */
    Set<GuidObjectKey> getKeys() throws AppendOnlyStoreException;
}
