package ru.unlocker.append.only.store.file;

import ru.unlocker.append.only.store.GuidObjectKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import net.java.truecommons.io.IntervalReadOnlyChannel;
import ru.unlocker.append.only.store.AppendOnlyStoreException;

/**
 * data file
 *
 * @author unlocker
 */
public class DataFile {

    /**
     * data file name
     */
    private final Path path;

    /**
     * data file
     *
     * @param path file path
     */
    public DataFile(Path path) {
        this.path = path;
    }

    /**
     * writes the object
     *
     * @param key
     * @param input object data
     * @return index
     * @throws AppendOnlyStoreException writing object exception
     */
    public IndexEntry putObject(GuidObjectKey key, ReadableByteChannel input) throws AppendOnlyStoreException {
        IndexEntry index = new IndexEntry();
        index.key = key;
        index.deleted = false;
        try (SeekableByteChannel data = Files.newByteChannel(path, EnumSet.of(APPEND, CREATE))) {
            index.offset = data.size();
            data.position(data.size());
            Path tempFile = Files.createTempFile("aos", ".tmp");
            try (SeekableByteChannel writer = Files.newByteChannel(tempFile, EnumSet.of(APPEND))) {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                MessageDigest digest = MessageDigest.getInstance("SHA-512");
                while (input.read(buffer) > 0) {
                    buffer.flip();
                    digest.update(buffer.array());
                    writer.write(buffer);
                    buffer.clear();
                }
                index.length = writer.size();
                index.hash = digest.digest();
                buffer = ByteBuffer.wrap(index.toBytes());
                data.write(buffer);
                writer.close();
                try (ReadableByteChannel reader = Files.newByteChannel(tempFile, EnumSet.of(READ))) {
                    buffer = ByteBuffer.allocate(1024);
                    while (reader.read(buffer) > 0) {
                        buffer.flip();
                        data.write(buffer);
                        buffer.clear();
                    }
                    return index;
                }
            } finally {
                if (tempFile != null) {
                    Files.delete(tempFile);
                }
            }

        } catch (IOException | NoSuchAlgorithmException e) {
            throw new AppendOnlyStoreException(e);
        }
    }

    /**
     * deletes object 
     *
     * @param key index entry
     * @throws AppendOnlyStoreException deletion exception
     */
    public void deleteObject(IndexEntry key) throws AppendOnlyStoreException {
        if (key.deleted) {
            return;
        }
        key.deleted = true;
        try {
            try (SeekableByteChannel output = Files.newByteChannel(path, EnumSet.of(READ, WRITE))) {
                ByteBuffer buffer = ByteBuffer.wrap(key.toBytes());
                output.position(key.offset);
                output.write(buffer);
            }
        } catch (IOException e) {
            throw new AppendOnlyStoreException(e);
        }
    }

    /**
     * retrieves object
     *
     * @param key index entry
     * @return object data
     * @throws AppendOnlyStoreException reading object exception
     */
    public ReadableByteChannel getObject(IndexEntry key) throws AppendOnlyStoreException {
        try {
            if (key.deleted) {
                throw new AppendOnlyStoreException("The object was deleted");
            }
            IntervalReadOnlyChannel result = new IntervalReadOnlyChannel(
                    Files.newByteChannel(path, EnumSet.of(READ)),
                    key.offset+IndexEntry.ENTRY_SIZE,
                    key.length);
            return result;
        } catch (IOException e) {
            throw new AppendOnlyStoreException(e);
        }
    }
}
