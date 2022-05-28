package MyDBMS;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SlottedPageTest {

    @Test
    void testEmptySlottedPage() {
        SlottedPage page = new SlottedPage(new Table("test_table", new Column[0]));

        assertEquals(0, page.getNumberOfEntries());
        assertTrue(page.getRecords().isEmpty());
        assertEquals(BufferPage.PAGE_SIZE - 2, page.getFreeSpaceSize());
        assertArrayEquals(new byte[BufferPage.PAGE_SIZE], page.toByteArray());
    }

    @Test
    void testAddRecord() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "hello world");

        page.addRecord(record);

        assertEquals(1, page.getNumberOfEntries());
        assertEquals(1, page.getRecords().size());
    }

    @Test
    void testAddRecordFromAnotherTable() {
        SlottedPage page = new SlottedPage(new Table("test_table", new Column[0]));
        Record record = new Record(new Table("another_table", new Column[0]));

        assertThrows(IllegalArgumentException.class, () -> page.addRecord(record));
    }

    @Test
    void testGetFreeSpaceSize() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", BufferPage.PAGE_SIZE - 6 - 1)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "");

        page.addRecord(record);

        assertEquals(0, page.getFreeSpaceSize());
    }

    @Test
    void testAddRecordInFullPage() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", BufferPage.PAGE_SIZE - 6 - 1)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "");

        page.addRecord(record);

        assertThrows(IndexOutOfBoundsException.class, () -> page.addRecord(record));
    }

    @Test
    void testToByteArray() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "hello world");

        page.addRecord(record);

        byte[] recordBytes = record.toByteArray();
        byte[] pageBytes = new byte[BufferPage.PAGE_SIZE];
        pageBytes[1] = 1;
        pageBytes[2] = (byte) (recordBytes.length >> 8);
        pageBytes[3] = (byte) (recordBytes.length & 0xFF);
        pageBytes[4] = (byte) ((pageBytes.length - recordBytes.length) >> 8);
        pageBytes[5] = (byte) ((pageBytes.length - recordBytes.length) & 0xFF);
        System.arraycopy(recordBytes, 0, pageBytes, pageBytes.length - recordBytes.length, recordBytes.length);
        assertArrayEquals(pageBytes, page.toByteArray());
    }

    @Test
    void testParseBytes() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table);
        record.setChar("test", "hello world");

        page.addRecord(record);

        page = new SlottedPage(table, page.toByteArray());

        assertEquals(1, page.getNumberOfEntries());
        assertEquals(record, page.getRecords().get(0));
    }

    @Test
    void testRemoveNonExistentRecord() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        assertFalse(page.removeRecord(new Record(table)));
    }

    @Test
    void testRemoveRecord() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table).setChar("test", "hello world");

        page.addRecord(record);

        assertTrue(page.removeRecord(record));
        assertTrue(page.getRecords().isEmpty());
        assertEquals(BufferPage.PAGE_SIZE - 2 - 4, page.getFreeSpaceSize());
    }

    @Test
    void testRemoveAll() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        SlottedPage page = new SlottedPage(table);

        Record record = new Record(table).setChar("test", "hello world");

        page.addRecord(record);
        page.removeAll();

        assertTrue(page.getRecords().isEmpty());
    }
}
