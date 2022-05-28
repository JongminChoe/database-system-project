package MyDBMS;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DBMSTest {

    @BeforeAll
    static void BeforeAll() {
        try {
            DBMS.getInstance().close();
            for (String path : new String[]{Dictionary.TABLE_DICTIONARY, Dictionary.ATTRIBUTE_DICTIONARY, "test_table"}) {
                Path p = Path.of(path);
                if (Files.exists(p)) {
                    Files.delete(p);
                }
            }
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @AfterAll
    static void afterAll() {
        try {
            DBMS.getInstance().close();
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @Test
    @Order(1)
    void testCreateTable() {
        DBMS.getInstance()
                .createTable("test_table")
                .addPrimaryColumn("primary_key", "VARCHAR", 255)
                .addColumn("column1", "VARCHAR", 16)
                .addColumn("column2", "CHAR", 16)
                .persist();

        assertNotNull(DBMS.getInstance().getDictionary().getTable("test_table"));
    }

    @Test
    @Order(2)
    void testCreateDuplicateTable() {
        assertFalse(DBMS.getInstance().createTable("test_table").persist());
    }

    @Test
    @Order(3)
    void testSelectOnEmptyTable() {
        assertEquals(0, DBMS.getInstance().queryTable("test_table").get().length);
    }

    @Test
    @Order(4)
    void testInsertRecord() {
        assertTrue(DBMS.getInstance().queryTable("test_table").insert("key1", "varchar", "char"));
        Record[] records = DBMS.getInstance().queryTable("test_table").get();
        assertEquals(1, records.length);
        assertEquals("key1", records[0].getVarchar("primary_key"));
        assertEquals("varchar", records[0].getVarchar("column1"));
        assertEquals("char            ", records[0].getChar("column2"));

        assertTrue(DBMS.getInstance().queryTable("test_table").insert("key2"));
        records = DBMS.getInstance().queryTable("test_table").get();
        assertEquals(2, records.length);
        assertEquals("key2", records[1].getVarchar("primary_key"));
        assertNull(records[1].getVarchar("column1"));
        assertNull(records[1].getChar("column2"));
    }

    @Test
    @Order(5)
    void testSelectWhereRecord() {
        Record[] records = DBMS.getInstance().queryTable("test_table").where("primary_key", "key1").get();
        assertEquals(1, records.length);
        assertEquals("key1", records[0].getVarchar("primary_key"));
    }

    @Test
    @Order(5)
    void testSelectWhereNotRecord() {
        Record[] records = DBMS.getInstance().queryTable("test_table").whereNot("primary_key", "key1").get();
        assertEquals(1, records.length);
        assertEquals("key2", records[0].getVarchar("primary_key"));
    }

    @Test
    @Order(5)
    void testSelectWhereNullRecord() {
        Record[] records = DBMS.getInstance().queryTable("test_table").whereNull("column1").get();
        assertEquals(1, records.length);
        assertEquals("key2", records[0].getVarchar("primary_key"));
    }

    @Test
    @Order(5)
    void testSelectWhereNotNullRecord() {
        Record[] records = DBMS.getInstance().queryTable("test_table").whereNotNull("column1").get();
        assertEquals(1, records.length);
        assertEquals("key1", records[0].getVarchar("primary_key"));
    }

    @Test
    @Order(6)
    void testDeleteRecord() {
        assertTrue(DBMS.getInstance().queryTable("test_table").where("primary_key", "key1").delete());
        assertEquals(1, DBMS.getInstance().queryTable("test_table").get().length);
        assertFalse(DBMS.getInstance().queryTable("test_table").whereNot("primary_key", "key2").delete());
        assertTrue(DBMS.getInstance().queryTable("test_table").whereNull("column1").delete());
        assertEquals(0, DBMS.getInstance().queryTable("test_table").get().length);
        assertFalse(DBMS.getInstance().queryTable("test_table").whereNotNull("column1").delete());
    }

    @Test
    @Order(7)
    void testDeleteTable() {
        DBMS.getInstance().deleteTable("test_table");

        assertThrows(IllegalArgumentException.class, () -> DBMS.getInstance().queryTable("test_table").get());
    }
}
