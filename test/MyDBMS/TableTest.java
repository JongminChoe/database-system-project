package MyDBMS;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    @BeforeAll
    static void BeforeAll() {
        Path path = Path.of("test_table");
        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                // e.printStackTrace();
            }
        }
    }

    @BeforeEach
    void BeforeEach() {
        DBMS.getInstance().getBufferManager().forceFlush();
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
    void testDifferentColumnOrder() {
        assertEquals(
                new Table("test_table", new Column[]{
                        new Column(Column.DataType.CHAR, "char_column", 16),
                        new Column(Column.DataType.VARCHAR, "varchar_column", 255)
                }),
                new Table("test_table", new Column[]{
                        new Column(Column.DataType.VARCHAR, "varchar_column", 255),
                        new Column(Column.DataType.CHAR, "char_column", 16)
                })
        );
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

        assertTrue(table.addRecord(record));

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

        assertTrue(table.addRecord(record));
        assertFalse(table.addRecord(record));

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

        assertTrue(table.addRecord(record1));
        assertTrue(table.addRecord(record2));

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

        assertArrayEquals(new Record[]{record1}, table.where("char_column", "char1           "));
        assertArrayEquals(new Record[]{record2}, table.where("char_column", "char2           "));
        assertArrayEquals(new Record[]{}, table.where("char_column", "char3           "));
    }

    @Test
    void testWhereCharNot() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record2}, table.whereNot("char_column", "char1           "));
        assertArrayEquals(new Record[]{record1}, table.whereNot("char_column", "char2           "));
        assertArrayEquals(new Record[]{record1, record2}, table.whereNot("char_column", "char3           "));
    }

    @Test
    void testWhereCharNull() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", null).setVarchar("varchar_column", null);

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record2}, table.where("char_column", null));
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

        assertArrayEquals(new Record[]{record1}, table.where("varchar_column", "varchar1"));
        assertArrayEquals(new Record[]{record2}, table.where("varchar_column", "varchar2"));
        assertArrayEquals(new Record[]{}, table.where("varchar_column", "varchar3"));
    }

    @Test
    void testWhereVarcharNot() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record2}, table.whereNot("varchar_column", "varchar1"));
        assertArrayEquals(new Record[]{record1}, table.whereNot("varchar_column", "varchar2"));
        assertArrayEquals(new Record[]{record1, record2}, table.whereNot("varchar_column", "varchar3"));
    }

    @Test
    void testWhereVarcharNull() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", null).setVarchar("varchar_column", null);

        table.addRecord(record1);
        table.addRecord(record2);

        assertArrayEquals(new Record[]{record2}, table.where("varchar_column", null));
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

    @Test
    void testDeleteOnEmptyTable() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });

        assertEquals(0, table.delete("char_column", "char1           "));
    }

    @Test
    void testDeleteWhereChar() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertEquals(1, table.delete("char_column", "char1           "));
        assertEquals(0, table.delete("char_column", "char1           "));

        assertEquals(0, table.where("char_column", "char1           ").length);
        assertArrayEquals(new Record[]{record2}, table.where("char_column", "char2           "));
    }

    @Test
    void testDeleteWhereCharNot() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertEquals(1, table.deleteNot("char_column", "char1           "));
        assertEquals(0, table.deleteNot("char_column", "char1           "));

        assertEquals(0, table.whereNot("char_column", "char1           ").length);
        assertArrayEquals(new Record[]{record1}, table.getAllRecords().toArray(Record[]::new));
    }

    @Test
    void testDeleteWhereVarchar() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertEquals(1, table.delete("varchar_column", "varchar1"));
        assertEquals(0, table.delete("varchar_column", "varchar1"));

        assertEquals(0, table.where("varchar_column", "varchar1").length);
        assertArrayEquals(new Record[]{record2}, table.where("varchar_column", "varchar2"));
    }

    @Test
    void testDeleteWhereVarcharNot() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "char_column", 16),
                new Column(Column.DataType.VARCHAR, "varchar_column", 16)
        });
        Record record1 = new Record(table).setChar("char_column", "char1").setVarchar("varchar_column", "varchar1");
        Record record2 = new Record(table).setChar("char_column", "char2").setVarchar("varchar_column", "varchar2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertEquals(1, table.deleteNot("varchar_column", "varchar1"));
        assertEquals(0, table.deleteNot("varchar_column", "varchar1"));

        assertEquals(0, table.whereNot("varchar_column", "varchar1").length);
        assertArrayEquals(new Record[]{record1}, table.getAllRecords().toArray(Record[]::new));
    }

    @Test
    void testDestroy() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.VARCHAR, "primary_key", 16)
        }, "primary_key");
        Record record1 = new Record(table).setVarchar("primary_key", "key1");
        Record record2 = new Record(table).setVarchar("primary_key", "key2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertTrue(table.destroy("key1"));
        assertFalse(table.destroy("key1"));
        assertNull(table.find("key1"));
        assertEquals(record2, table.find("key2"));
    }

    @Test
    void testTruncate() {
        Table table = new Table("test_table", new Column[]{
                new Column(Column.DataType.VARCHAR, "primary_key", 16)
        }, "primary_key");
        Record record1 = new Record(table).setVarchar("primary_key", "key1");
        Record record2 = new Record(table).setVarchar("primary_key", "key2");

        table.addRecord(record1);
        table.addRecord(record2);

        assertTrue(table.truncate());

        assertEquals(0, table.getAllRecords().count());
    }
}
