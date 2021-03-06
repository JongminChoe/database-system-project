package MyDBMS;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class BufferPageTest {

    @BeforeAll
    static void BeforeAll() {
        File file = new File("test_table");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    void testPageMarkedAsDirtyWhenPayloadChanges() {
        assertDoesNotThrow(() -> {
            BufferPage page = DBMS.getInstance().getBufferManager().getEmptyPage("test_table", 0);

            assertFalse(page.isDirty());

            page.setPayload(new byte[]{ 0, 1, 2, 3 });

            assertTrue(page.isDirty());
        });
    }

    @Test
    void testIndexOutOfFileBound() {
        assertThrows(IOException.class, () -> DBMS.getInstance().getBufferManager().getPage("test_table", 0));
    }
}
