package ru.unlocker.append.only.store.file;

import ru.unlocker.append.only.store.GuidObjectKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * index entry
 *
 * @author unlocker
 */
public class IndexEntry implements Serializable {

    /**
     * a size of entry
     */
    public final static int ENTRY_SIZE = 426;

    /**
     * unique identifier
     */
    GuidObjectKey key;

    /**
     * deletion flag
     */
    boolean deleted;

    /**
     * hash bytes
     */
    byte[] hash;

    /**
     * offset from the begin of file
     */
    long offset;

    /**
     * length of the data object
     */
    long length;

    /**
     * converts entry to the bytes array
     *
     * @return byte array
     */
    public byte[] toBytes() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(ENTRY_SIZE);
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(this);
        } catch (IOException ex) {
            return null;
        }
        return baos.toByteArray();
    }
}
