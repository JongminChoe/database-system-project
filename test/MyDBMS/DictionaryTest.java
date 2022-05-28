package MyDBMS;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryTest {

    @BeforeEach
    void BeforeEach() {
        DBMS.getInstance().getBufferManager().forceFlush();
        DBMS.getInstance().getDictionary().reload();
    }

    @Test
    void testNotExistentTable() {
        assertNull(DBMS.getInstance().getDictionary().getTable("non_existent_table"));
    }

    @Test
    void testInvalidTableName() {
        assertThrows(IllegalArgumentException.class, () -> DBMS.getInstance().getDictionary().createTable(".invalid", new Column[0], null));
        assertThrows(IllegalArgumentException.class, () -> DBMS.getInstance().getDictionary().createTable("inv*lid", new Column[0], null));
    }

    @Test
    void testCreateTable() {
        DBMS.getInstance().getDictionary().createTable("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key");

        assertEquals(new Table("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key"), DBMS.getInstance().getDictionary().getTable("test_table"));
        assertNotNull(DBMS.getInstance().getDictionary().getTable(Dictionary.TABLE_DICTIONARY).find("test_table"));
    }

    @Test
    void testCreateDuplicateTable() {
        assertDoesNotThrow(() -> DBMS.getInstance().getDictionary().createTable("test_table", new Column[0], null));
        assertThrows(IllegalArgumentException.class, () -> DBMS.getInstance().getDictionary().createTable("test_table", new Column[0], null));
    }

    @Test
    void testDeleteTable() {
        DBMS.getInstance().getDictionary().createTable("test_table", new Column[]{
                new Column(Column.DataType.CHAR, "primary_key", 16)
        }, "primary_key");

        DBMS.getInstance().getDictionary().deleteTable("test_table");

        assertNull(DBMS.getInstance().getDictionary().getTable("test_table"));
        assertNull(DBMS.getInstance().getDictionary().getTable(Dictionary.TABLE_DICTIONARY).find("test_table"));
        assertEquals(0, DBMS.getInstance().getDictionary().getTable(Dictionary.ATTRIBUTE_DICTIONARY).where("table", "test_table").length);
    }
}
