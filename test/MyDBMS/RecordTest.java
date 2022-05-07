package MyDBMS;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    @Test
    void testNoColumns() {
        assertArrayEquals(new byte[0], new Record(new Column[0]).getBytes());
    }

    @Test
    void testUnsettedColumn() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });

        assertNull(record.getChar("test"));
    }

    @Test
    void testNonExistentColumn() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });

        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> record.getChar("non_existent_column"));
        assertEquals("Column [non_existent_column] does not exists", throwable.getMessage());
    }

    @Test
    void testSingleCharColumnRecord() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        record.setChar("test", "hello world");

        assertEquals("hello world     ", record.getChar("test"));
    }

    @Test
    void testSingleVarcharColumnRecord() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.VARCHAR, "test", 16)
        });
        record.setVarchar("test", "hello world");

        assertEquals("hello world", record.getVarchar("test"));
    }

    @Test
    void testSetCharColumnToNull() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        record.setChar("test", "hello world");
        record.setChar("test", null);

        assertNull(record.getChar("test"));
    }

    @Test
    void testSetVarcharColumnToNull() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.VARCHAR, "test", 16)
        });
        record.setVarchar("test", "hello world");
        record.setVarchar("test", null);

        assertNull(record.getVarchar("test"));
    }

    @Test
    void testSingleCharColumnRecordToBytes() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        record.setChar("test", "hello world");

        assertArrayEquals("hello world     \0".getBytes(), record.getBytes());
    }

    @Test
    void testSingleVarcharColumnRecordToBytes() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.VARCHAR, "test", 16)
        });
        record.setVarchar("test", "hello world");

        byte[] expected = "\0\0\0\0\0hello world".getBytes();
        expected[1] = 5;
        expected[3] = (byte) "hello world".length();

        assertArrayEquals(expected, record.getBytes());
    }

    @Test
    void testSetInvalidColumnType() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.VARCHAR, "varchar_type", 16)
        });

        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> record.setChar("varchar_type", "hello world"));
        assertEquals("Column [varchar_type] is not CHAR type", throwable.getMessage());
    }

    @Test
    void testSingleCharColumnRecordFromBytes() {
        Record record = new Record(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        }, "hello world     \0".getBytes());

        assertEquals("hello world     ", record.getChar("test"));
    }
    
    @Test
    void testSingleVarcharColumnRecordFromBytes() {
        byte[] payload = "\0\0\0\0\0hello world".getBytes();
        payload[1] = 5;
        payload[3] = (byte) "hello world".length();
        
        Record record = new Record(new Column[]{
                new Column(Column.DataType.VARCHAR, "test", 16)
        }, payload);

        assertEquals("hello world", record.getVarchar("test"));
    }
}