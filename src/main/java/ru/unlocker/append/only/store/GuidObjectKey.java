package ru.unlocker.append.only.store;

import java.io.Serializable;
import java.util.UUID;

/**
 * Based GUID object key
 *
 * @author unlocker
 */
public class GuidObjectKey implements Serializable {

    /**
     * @return creates unique key
     */
    public static GuidObjectKey createNew() {
        return new GuidObjectKey(UUID.randomUUID());
    }

    /**
     * identifier
     */
    private final UUID guid;

    /**
     * based GUID object key
     *
     * @param guid identifier
     */
    protected GuidObjectKey(UUID guid) {
        this.guid = guid;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + (this.guid != null ? this.guid.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final GuidObjectKey other = (GuidObjectKey) obj;
        return this.guid == other.guid || (this.guid != null && this.guid.equals(other.guid));
    }

    @Override
    public String toString() {
        return guid.toString();
    }
}
