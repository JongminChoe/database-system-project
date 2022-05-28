package MyDBMS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class Dictionary {
    public static final String TABLE_DICTIONARY = ".table";
    public static final String ATTRIBUTE_DICTIONARY = ".attribute";

    private final HashMap<String, Table> tables;

    private Dictionary() {
        this.tables = new HashMap<>();
        this.reload();
    }

    public static Dictionary getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final Dictionary INSTANCE = new Dictionary();
    }

    public void reload() {
        this.tables.clear();

        Table attributeTable = this.addDefaultAttributeTable();
        Table tableTable = this.addDefaultTableTable();

        HashMap<String, ArrayList<Column>> attributesByTable = this.collectAttributes(attributeTable);
        this.registerTables(tableTable, attributesByTable);
    }

    private Table addDefaultAttributeTable() {
        Table table = new Table(ATTRIBUTE_DICTIONARY, new Column[]{
                new Column(Column.DataType.VARCHAR, "table", 255),
                new Column(Column.DataType.VARCHAR, "name", 255),
                new Column(Column.DataType.CHAR, "type", 1),
                new Column(Column.DataType.CHAR, "size", 2),
                new Column(Column.DataType.CHAR, "notnull", 1),
                new Column(Column.DataType.CHAR, "position", 2)
        });
        this.tables.put(table.getTableName(), table);
        return table;
    }

    private Table addDefaultTableTable() {
        Table table = new Table(TABLE_DICTIONARY, new Column[]{
                new Column(Column.DataType.VARCHAR, "name", 255),
                new Column(Column.DataType.VARCHAR, "primary_key", 255),
                new Column(Column.DataType.CHAR, "nattrs", 2)
        }, "name");
        this.tables.put(table.getTableName(), table);
        return table;
    }

    private HashMap<String, ArrayList<Column>> collectAttributes(Table attributeTable) {
        return attributeTable
                .getAllRecords()
                .collect(Collectors.groupingBy(
                        record -> record.getVarchar("table"),
                        HashMap::new,
                        Collectors.mapping(
                                record -> new Column(
                                        Column.DataType.values()[record.getChar("size").getBytes()[0]],
                                        record.getVarchar("name"),
                                        ByteBuffer.wrap(record.getChar("size").getBytes()).getShort()
                                ),
                                Collectors.toCollection(ArrayList::new)
                        )
                ));
    }

    private void registerTables(Table tableTable, HashMap<String, ArrayList<Column>> attributesByTable) {
        this.tables.putAll(tableTable
                .getAllRecords()
                .collect(Collectors.toMap(
                        record -> record.getVarchar("name"),
                        record -> new Table(
                                record.getVarchar("name"),
                                attributesByTable.getOrDefault(record.getVarchar("name"), new ArrayList<>(0)).toArray(Column[]::new),
                                record.getVarchar("primary_key")
                        )
                ))
        );
    }

    private void addTable(Table table) {
        this.tables.put(table.getTableName(), table);

        this.getTable(TABLE_DICTIONARY).addRecord(
                new Record(this.getTable(TABLE_DICTIONARY))
                        .setVarchar("name", table.getTableName())
                        .setVarchar("primary_key", table.getPrimaryColumn())
                        .setChar("nattrs", new String(ByteBuffer.allocate(2).putShort((short) table.getColumns().length).array()))
        );

        Column[] columns = table.getColumns();
        for (int index = 0; index < columns.length; index++) {
            Column column = columns[index];
            this.getTable(ATTRIBUTE_DICTIONARY).addRecord(
                    new Record(this.getTable(ATTRIBUTE_DICTIONARY))
                            .setVarchar("table", table.getTableName())
                            .setVarchar("name", column.getName())
                            .setChar("type", new String(new byte[]{(byte) column.getType().ordinal()}))
                            .setChar("size", new String(ByteBuffer.allocate(2).putShort((short) column.getSize()).array()))
                            .setChar("notnull", new String(new byte[]{0}))
                            .setChar("position", new String(ByteBuffer.allocate(2).putShort((short) index).array()))
            );
        }
    }

    public Table createTable(String tableName, Column[] columns, String primaryColumn) {
        if (this.tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table [" + tableName + "] already exists");
        }
        if (!tableName.matches("[_a-zA-Z0-9]+")) {
            throw new IllegalArgumentException("Invalid table name");
        }
        Table table = new Table(tableName, columns, primaryColumn);
        this.addTable(table);
        return table;
    }

    public Table getTable(String tableName) {
        return this.tables.get(tableName);
    }

    public void deleteTable(String tableName) {
        this.tables.remove(tableName).forceFlush();
        this.getTable(TABLE_DICTIONARY).destroy(tableName);
        this.getTable(ATTRIBUTE_DICTIONARY).delete("table", tableName);
    }

    public void flush() throws IOException {
        this.getTable(ATTRIBUTE_DICTIONARY).flush();
        this.getTable(TABLE_DICTIONARY).flush();
    }
}
