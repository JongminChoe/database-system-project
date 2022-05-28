package MyDBMS;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FilePoolTest {
    private static final String TABLE_NAME = "test_table";

    @BeforeAll
    static void BeforeAll() {
        try {
            FilePool.getInstance().close();
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @Test
    @Order(1)
    void testWrite() {
        assertDoesNotThrow(() -> FilePool.getInstance().write(TABLE_NAME, 0, new byte[]{0, 1, 2, 3}));
    }

    @Test
    @Order(2)
    void testWriteOffset() {
        assertDoesNotThrow(() -> FilePool.getInstance().write(TABLE_NAME, 8, new byte[]{ 4, 5, 6, 7 }));
    }

    @Test
    @Order(3)
    void testRead() {
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 0, 1, 2, 3 }, FilePool.getInstance().read(TABLE_NAME, 0, 4)));
    }

    @Test
    @Order(4)
    void testReadOffset() {
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 0, 0, 0, 0 }, FilePool.getInstance().read(TABLE_NAME, 4, 4)));
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 4, 5, 6, 7 }, FilePool.getInstance().read(TABLE_NAME, 8, 4)));
    }

    @Test
    @Order(5)
    void testReadOutOfFileSize() {
        assertThrows(IOException.class, () -> assertNull(FilePool.getInstance().read(TABLE_NAME, 12, 4)));
    }

    @Test
    @Order(6)
    void testSize() {
        assertEquals(1, FilePool.getInstance().size());
    }

    @Test
    @Order(7)
    void testDelete() {
        File file = new File("test_table");
        assertTrue(file.exists());

        assertDoesNotThrow(() -> FilePool.getInstance().delete(TABLE_NAME));

        assertFalse(file.exists());
    }
}
