package MyDBMS;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class BufferPageTest {

    @Test
    void testPageMustBeDirtyWhenPayloadWasPassedToTheConstructor() {
        byte[] payload = new byte[BufferPage.PAGE_SIZE];
        BufferPage page = new BufferPage("TestPage", 0, payload);

        assertTrue(page.isDirty());
    }

    @Test
    void testPageFromNonExistentFile() {
        assertThrows(FileNotFoundException.class, () -> new BufferPage("NonExistentFileName", 0));
    }

    @Test
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
}