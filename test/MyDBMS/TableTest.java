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
    void testGetColumns() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });

        assertArrayEquals(new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        }, table.getColumns());
    }

    @Test
    void testGetPrimaryColumn() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key");

        assertEquals("primary_key", table.getPrimaryColumn());
    }

    @Test
    void testInvalidPrimaryColumn() {
        assertThrows(IllegalArgumentException.class, () -> new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "another_key"));
    }

    @Test
    void testDuplicateColumnName() {
        assertThrows(IllegalStateException.class, () -> new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16),
                new Column(Column.DataType.VARCHAR, "primary_key", 255)
        }));
    }

    @Test
    void testEmptyTable() {
        Table table = new Table("test_table", new Column[0]);

        assertEquals(0, table.getAllRecords().count());
    }

    @Test
    void testAddSingleRecord() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        Record record = new Record(table);
        record.setChar("test", "hello world");

        table.addRecord(record);

        Record[] records = table.getAllRecords().toArray(Record[]::new);
        assertArrayEquals(new Record[]{record}, records);
    }

    @Test
    void testAddDuplicatePrimaryKey() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.VARCHAR, "primary_key", 16)
        }, "primary_key");
        Record record = new Record(table);
        record.setVarchar("primary_key", "hello world");

        table.addRecord(record);
        table.addRecord(record);

        Record[] records = table.getAllRecords().toArray(Record[]::new);
        assertArrayEquals(new Record[]{record}, records);
    }

    @Test
    void testAddMultipleRecords() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "test", 16)
        });
        Record record1 = new Record(table).setChar("test", "hello world1");
        Record record2 = new Record(table).setChar("test", "hello world2");

        table.addRecord(record1);
        table.addRecord(record2);

        Record[] records = table.getAllRecords().toArray(Record[]::new);
        assertArrayEquals(new Record[]{record1, record2}, records);
    }

    @Test
    void testWhereChar() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record1}, table.whereChar("char_column", "char1           "));
        assertArrayEquals(new Record[]{record2}, table.whereChar("char_column", "char2           "));
        assertArrayEquals(new Record[]{}, table.whereChar("char_column", "char3           "));
    }

    @Test
    void testWhereVarchar() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record1}, table.whereVarchar("varchar_column", "varchar1"));
        assertArrayEquals(new Record[]{record2}, table.whereVarchar("varchar_column", "varchar2"));
        assertArrayEquals(new Record[]{}, table.whereVarchar("varchar_column", "varchar3"));
    }

    @Test
    void testFindRecord() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.VARCHAR, "primary_key", 16)
        }, "primary_key");
        Record record1 = new Record(table).setVarchar("primary_key", "key1");
        Record record2 = new Record(table).setVarchar("primary_key", "key2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertEquals(record1, table.find("key1"));
        assertEquals(record2, table.find("key2"));
        assertNull(table.find("key3"));
    }
}
