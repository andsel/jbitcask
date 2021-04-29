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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void testListDataFiles() {
        final boolean res = IntStream.range(8, 13) // Generate a list of files from 8->12
                .mapToObj(i -> tempFolder.resolve(i + ".bitcask.data")) // Create each of the files
                .allMatch(this::createFile);
        assertTrue(res, "All files should be created");

        // Now use the listDataFiles to scan the dir
        final List<Path> expectedFiles = JBitCask.listDataFiles(tempFolder.toString(), null, null);
        assertTrue(expectedFiles.isEmpty(), "No files should be found");
    }

    private boolean createFile(Path path) {
        try {
            return path.toFile().createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    // Test that readableFiles will not return the currently active
    // write or merge file by mistake if they change in between fetching them
    // and listing the files in the directory.
    @Test
    public void testListDataFilesRace() {
        final boolean res = IntStream.range(1, 6) // Generate a list of files from 1->5
                .mapToObj(i -> tempFolder.resolve(i + ".bitcask.data")) // Create each of the files
                .allMatch(this::createFile);
        assertTrue(res, "All files should be created");

        // Faking 4 as merge file, 5 as write file,
        // then switching to 6 as merge, 7 as write
        LockOperations mockLockOperations = new LockOperations() {
            private String fakeActiveMergefile;
            private String fakeActiveWritefile;

            @Override
            String readActivefile(LockType type, String dirname) {
                if (type == LockType.MERGE) {
                    if (fakeActiveMergefile != null) {
                        return fakeActiveMergefile;
                    }
                    int n = 4;
                    // Next time return file + 2
                    createFile(tempFolder.resolve((n + 2) + ".bitcask.data"));
                    fakeActiveMergefile = (n + 2) + ".bitcask.data";
                    return n + ".bitcask.data";
                } else if (type == LockType.WRITE) {
                    if (fakeActiveWritefile != null) {
                        return fakeActiveWritefile;
                    }
                    int n = 5;
                    // Next time return file + 2
                    createFile(tempFolder.resolve((n + 2) + ".bitcask.data"));
                    fakeActiveWritefile = (n + 2) + ".bitcask.data";
                    return n + ".bitcask.data";
                } else {
                    throw new IllegalArgumentException("Called with " + type + " while expected MERGE or WRITE");
                }
            }
        };

        final JBitCask.FileLister sut = new JBitCask.FileLister(mockLockOperations);
        final List<Path> readFiles = sut.readableFiles(tempFolder.toString());

        // Verify
        Collections.sort(readFiles);
        final List<Path> expectedFileNames = IntStream.range(1, 6)
                .mapToObj(i -> tempFolder.resolve(i + ".bitcask.data"))
                .collect(Collectors.toList());
        assertEquals(expectedFileNames, readFiles);
    }
}