package MyDBMS;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryTest {

    @BeforeEach
    void BeforeEach() {
        BufferManager.getInstance().forceFlush();
        Dictionary.getInstance().reload();
    }

    @Test
    void testNotExistentTable() {
        assertNull(Dictionary.getInstance().getTable("non_existent_table"));
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
        assertNotNull(Dictionary.getInstance().getTable(Dictionary.TABLE_DICTIONARY).find("test_table"));
    }

    @Test
    void testCreateDuplicateTable() {
        assertDoesNotThrow(() -> Dictionary.getInstance().createTable("test_table", new Column[0], null));
        assertThrows(IllegalArgumentException.class, () -> Dictionary.getInstance().createTable("test_table", new Column[0], null));
    }

    @Test
    void testDeleteTable() {
        Dictionary.getInstance().createTable("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key");

        Dictionary.getInstance().deleteTable("test_table");

        assertNull(Dictionary.getInstance().getTable("test_table"));
        assertNull(Dictionary.getInstance().getTable(Dictionary.TABLE_DICTIONARY).find("test_table"));
        assertEquals(0, Dictionary.getInstance().getTable(Dictionary.ATTRIBUTE_DICTIONARY).where("table", "test_table").length);
    }
}
