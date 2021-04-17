package org.dna.jbitcask;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class JBitCaskTest {

    @TempDir
    Path tempFolder;

    @BeforeEach
    public void setUp() {
        tempFolder.resolve("bitcask").toFile().mkdirs();
    }

    @Test
    public void testRoundtrip() throws InterruptedException, TimeoutException, IOException, LockOperations.AlreadyLockedException {
        final Properties props = new Properties();
        props.put("read_write", Boolean.TRUE);
        final JBitCask bitCask = JBitCask.open(tempFolder.toString(), new Options(props));
        wrapPut(bitCask, "k", "v");
        String value = wrapGet(bitCask, "k");
        assertEquals("v", value);

        wrapPut(bitCask, "k2", "v2");
        wrapPut(bitCask, "k", "v3");

        value = wrapGet(bitCask, "k2");
        assertEquals("v2", value);

        value = wrapGet(bitCask, "k");
        assertEquals("v3", value);

        bitCask.close();
    }

    private String wrapGet(JBitCask bitCask, String key) throws IOException {
        return new String(bitCask.get(key.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    private void wrapPut(JBitCask bitCask, String key, String value)
            throws IOException, LockOperations.AlreadyLockedException, InterruptedException {
        bitCask.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testWriteLockPerms() throws IOException, InterruptedException, TimeoutException, LockOperations.AlreadyLockedException {
        final Properties props = new Properties();
        props.put("read_write", Boolean.TRUE);
        final JBitCask bitCask = JBitCask.open(tempFolder.toString(), new Options(props));

        wrapPut(bitCask, "k", "v");

        final Set<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(tempFolder.resolve("bitcask.write.lock"));
        final String permissions = PosixFilePermissions.toString(filePermissions);
        assertEquals("rw-------", permissions);
    }

    
}