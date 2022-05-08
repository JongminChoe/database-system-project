package MyDBMS;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BufferPageTest {

    @BeforeAll
    static void BeforeAll() {
        File file = new File("TestPage");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    @Order(1)
    void testPageMustBeDirtyWhenPayloadWasPassedToTheConstructor() {
        byte[] payload = new byte[BufferPage.PAGE_SIZE];
        BufferPage page = new BufferPage("TestPage", 0, payload);

        assertTrue(page.isDirty());
    }

    @Test
    @Order(2)
    void testInvalidPayloadSizeInConstructor() {
        assertThrows(AssertionError.class, () -> new BufferPage("TestPage", 0, new byte[BufferPage.PAGE_SIZE + 1]));
    }

    @Test
    @Order(3)
    void testPageFromNonExistentFile() {
        assertThrows(FileNotFoundException.class, () -> new BufferPage("NonExistentFileName", 0));
    }

    @Test
    @Order(4)
    void testPayloadEqualsOnIndex0() {
        // dummy payload
        byte[] payload = new byte[BufferPage.PAGE_SIZE];
        Arrays.fill(payload, (byte) 'A');
        BufferPage page = new BufferPage("TestPage", 0, payload);

        assertDoesNotThrow(page::flush);
        assertFalse(page.isDirty());

        BufferPage newPage = null;
        try {
            newPage = new BufferPage("TestPage", 0);
        } catch (IOException e) {
            // e.printStackTrace();
        }
        assert newPage != null;
        assertArrayEquals(payload, newPage.getPayload());
    }

    @Test
    @Order(5)
    void testPayloadEqualsOnIndex1() {
        // dummy payload
        byte[] payload = new byte[BufferPage.PAGE_SIZE];
        Arrays.fill(payload, (byte) 'B');
        BufferPage page = new BufferPage("TestPage", 1, payload);

        assertDoesNotThrow(page::flush);
        assertFalse(page.isDirty());

        BufferPage newPage = null;
        try {
            newPage = new BufferPage("TestPage", 1);
        } catch (IOException e) {
            // e.printStackTrace();
        }
        assert newPage != null;
        assertArrayEquals(payload, newPage.getPayload());
    }

    @Test
    @Order(6)
    void testIndexOutOfFileBound() {
        assertThrows(IndexOutOfBoundsException.class, () -> new BufferPage("TestPage", 2));
    }
}