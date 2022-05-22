package MyDBMS;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryTest {

    @Test
    void testEmptyDictionary() {
        assertNull(Dictionary.getInstance().getTable("test_table"));
    }

    @Test
    void testInvalidTableName() {
        assertThrows(IllegalArgumentException.class, () -> Dictionary.getInstance().createTable(".invalid", new Column[0], null));
        assertThrows(IllegalArgumentException.class, () -> Dictionary.getInstance().createTable("inv*lid", new Column[0], null));
    }

    @Test
    void testCreateTable() {
        Dictionary.getInstance().createTable("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key");

        assertEquals(new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key"), Dictionary.getInstance().getTable("test_table"));
        assertEquals(1, Dictionary.getInstance().getTable(Dictionary.TABLE_DICTIONARY).whereVarchar("name", "test_table").length);
    }
}
