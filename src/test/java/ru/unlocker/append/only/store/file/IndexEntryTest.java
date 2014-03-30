package ru.unlocker.append.only.store.file;

import ru.unlocker.append.only.store.GuidObjectKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import static org.hamcrest.CoreMatchers.*;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author unlocker
 */
public class IndexEntryTest {

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Test
    public void testSerialization() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        byte[] hash = digest.digest("test".getBytes());
        long length = 450L;
        long offset = 49L;
        GuidObjectKey key = GuidObjectKey.createNew();
        IndexEntry entry = new IndexEntry();
        entry.key = key;
        entry.hash = hash;
        entry.length = length;
        entry.offset = offset;
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(byteStream)) {
            output.writeObject(entry);
        }
        
        System.out.println(byteStream.size());
        IndexEntry result;
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(byteStream.toByteArray()))) {
            result = (IndexEntry) input.readObject();
        }

        assertNotNull(result);
        assertThat(result.key, equalTo(key));
        assertFalse(result.deleted);
        assertArrayEquals(hash, result.hash);
        assertThat(result.length, equalTo(length));
        assertThat(result.offset, equalTo(offset));
    }
}
