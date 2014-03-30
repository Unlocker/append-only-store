package ru.unlocker.append.only.store.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import ru.unlocker.append.only.store.AppendOnlyStoreException;
import ru.unlocker.append.only.store.GuidObjectKey;

/**
 *
 * @author unlocker
 */
public class AppendOnlyStoreFileTest {

    private Path tempDir;
    private AppendOnlyStoreFile store;

    @Before
    public void setUp() throws IOException, AppendOnlyStoreException {
        tempDir = Files.createTempDirectory("aos");
        store = new AppendOnlyStoreFile(tempDir.toString());
    }

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(tempDir.toString(), AppendOnlyStoreFile.DATA_FILE));
        Files.deleteIfExists(Paths.get(tempDir.toString(), AppendOnlyStoreFile.INDEX_FILE));
        Files.deleteIfExists(tempDir);
    }

    @Test
    public void putObjectTest() throws Exception {
        String value = "aos_";
        List<GuidObjectKey> keys = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            GuidObjectKey key = store.generateKey();
            keys.add(key);
            value += Integer.toString(i);
            store.putObject(key, Channels.newChannel(new ByteArrayInputStream(value.getBytes())));
        }

        assertThat(store.getKeys(), notNullValue());
        assertThat(store.getKeys().size(), equalTo(keys.size()));

        try (ReadableByteChannel obj = store.getObject(keys.get(2))) {
            assertThat(obj, notNullValue());
            ByteBuffer buf = ByteBuffer.allocate(128);
            obj.read(buf);
            buf.flip();
            final String result = bufferToString(buf);
            assertThat(result, equalTo("aos_012"));
        }
    }

    @Test
    public void putSingleObjectTest() throws Exception {
        String value = "aos";
        List<GuidObjectKey> keys = new ArrayList<>();

        GuidObjectKey key = store.generateKey();
        keys.add(key);
        store.putObject(key, Channels.newChannel(new ByteArrayInputStream(value.getBytes("UTF-8"))));

        assertThat(store.getKeys(), notNullValue());
        assertThat(store.getKeys().size(), equalTo(keys.size()));

        try (ReadableByteChannel obj = store.getObject(keys.get(0))) {
            assertThat(obj, notNullValue());
            ByteBuffer buf = ByteBuffer.allocate(128);
            obj.read(buf);
            buf.flip();
            final String result = bufferToString(buf);
            assertThat(result, equalTo("aos"));
        }
    }
    
    @Test(expected = AppendOnlyStoreException.class)
    public void deletedObjectTest() throws UnsupportedEncodingException, AppendOnlyStoreException{
        String value = "aos";
        GuidObjectKey key = store.generateKey();
        store.putObject(key, Channels.newChannel(new ByteArrayInputStream(value.getBytes("UTF-8"))));
        store.deleteObject(key);
        store.getObject(key);
    }
    
    @Test(expected = AppendOnlyStoreException.class)
    public void nonExistentObjectTest() throws UnsupportedEncodingException, AppendOnlyStoreException{
        String value = "aos";
        GuidObjectKey key = store.generateKey();
        store.getObject(key);
    }

    public static String bufferToString(ByteBuffer buf) {
        Charset cs = Charset.defaultCharset();
        return cs.decode(buf).toString();
    }
}
