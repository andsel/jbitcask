package org.dna.jbitcask;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
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
        bitCask.put("k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
        String value = new String(bitCask.get("k".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        assertEquals("v", value);

        bitCask.put("k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8));
        bitCask.put("k".getBytes(StandardCharsets.UTF_8), "v3".getBytes(StandardCharsets.UTF_8));

        value = new String(bitCask.get("k2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        assertEquals("v2", value);

        value = new String(bitCask.get("k".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        assertEquals("v3", value);

        bitCask.close();
    }
}