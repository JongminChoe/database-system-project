package MyDBMS;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    @BeforeAll
    static void BeforeAll() {
        File file = new File("test_table");
        if (file.exists()) {
            file.delete();
        }
    }

    @BeforeEach
    void BeforeEach() {
        BufferManager.getInstance().forceFlush();
    }

    @Test
    void testGetTableName() {
        Table table = new Table("test_table", new Column[0]);

        assertEquals("test_table", table.getTableName());
    }

    @Test
    void testGetAllRecordsFromEmptyBufferPage() {
        Table table = new Table("test_table", new Column[0]);

        assertEquals(0, table.getAllRecords().count());
    }

    @Test
    void testGetAllRecordsFromSingleRecordBufferPage() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "hello world");

        page.addRecord(record);

        assertDoesNotThrow(() -> BufferManager.getInstance().getEmptyPage(table.getTableName(), 0).setPayload(page.toByteArray()));

        Record[] records = table.getAllRecords().toArray(Record[]::new);
        assertEquals(1, records.length);
        assertEquals(record, records[0]);
    }
}