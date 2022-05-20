package MyDBMS;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FilePoolTest {

    @Test
    @Order(1)
    void testWrite() {
        assertDoesNotThrow(() -> FilePool.getInstance().write("test_file", 0, new byte[]{ 0, 1, 2, 3 }));
    }

    @Test
    @Order(2)
    void testWriteOffset() {
        assertDoesNotThrow(() -> FilePool.getInstance().write("test_file", 8, new byte[]{ 4, 5, 6, 7 }));
    }

    @Test
    @Order(3)
    void testRead() {
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 0, 1, 2, 3 }, FilePool.getInstance().read("test_file", 0, 4)));
    }

    @Test
    @Order(4)
    void testReadOffset() {
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 0, 0, 0, 0 }, FilePool.getInstance().read("test_file", 4, 4)));
        assertDoesNotThrow(() -> assertArrayEquals(new byte[]{ 4, 5, 6, 7 }, FilePool.getInstance().read("test_file", 8, 4)));
    }

    @Test
    @Order(5)
    void testSize() {
        assertEquals(1, FilePool.getInstance().size());
    }

    @Test
    @Order(6)
    void testDelete() {
        File file = new File("test_file");
        assertTrue(file.exists());

        assertDoesNotThrow(() -> FilePool.getInstance().delete("test_file"));

        assertFalse(file.exists());
    }
}