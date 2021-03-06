package MyDBMS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DBMS implements Closeable {
    private final FilePool filePool;
    private final BufferManager bufferManager;
    private Dictionary dictionary;

    private DBMS() {
        this(new FilePool(), new BufferManager());
    }

    public DBMS(FilePool filePool, BufferManager bufferManager) {
        this.filePool = filePool;
        this.bufferManager = bufferManager;
    }

    public static DBMS getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final DBMS INSTANCE = new DBMS();
    }

    public FilePool getFilePool() {
        return this.filePool;
    }

    public BufferManager getBufferManager() {
        return this.bufferManager;
    }

    public synchronized Dictionary getDictionary() {
        // Dictionary must not be loaded in the constructor
        // due to cyclic reference of getBufferManager()
        if (this.dictionary == null) {
            this.dictionary = new Dictionary();
        }
        return this.dictionary;
    }

    public TableBuilder createTable(String tableName) {
        return new TableBuilder(tableName);
    }

    public class TableBuilder {
        private final String tableName;
        private final List<Column> columns;
        private String primaryColumn;

        public TableBuilder(String tableName) {
            this.tableName = tableName;
            this.columns = new ArrayList<>();
        }

        public TableBuilder addColumn(String name, String type, int size) {
            return this.addColumn(name, type, size, false);
        }

        public TableBuilder addColumn(String name, String type, int size, boolean notnull) {
            this.columns.add(new Column(
                    Arrays.stream(Column.DataType.values()).filter(dataType -> dataType.getValue().equalsIgnoreCase(type)).findAny().orElseThrow(),
                    name,
                    size,
                    notnull
            ));
            return this;
        }

        public TableBuilder addPrimaryColumn(String name, String type, int size) {
            this.primaryColumn = name;
            return this.addColumn(name, type, size, true);
        }

        @Override
        public String toString() {
            StringBuilder query = new StringBuilder("CREATE TABLE ").append(this.tableName).append(" (");
            for (int i = 0; i < this.columns.size(); i++) {
                Column column = this.columns.get(i);
                if (i > 0) {
                    query.append(",");
                }
                query.append("\n  ").append(column.getName())
                        .append(" ").append(column.getType().getValue())
                        .append("(").append(column.getSize()).append(")");
                if (column.getName().equals(this.primaryColumn)) {
                    query.append(" PRIMARY KEY");
                }
            }
            query.append("\n)");

            return query.toString();
        }

        public boolean persist() {
            try {
                return DBMS.this.getDictionary().createTable(this.tableName, this.columns.toArray(Column[]::new), this.primaryColumn) != null;
            } catch (Exception e) {
                return false;
            }
        }
    }

    public void deleteTable(String tableName) {
        this.getDictionary().deleteTable(tableName);
    }

    public QueryBuilder queryTable(String tableName) {
        return new QueryBuilder(tableName);
    }

    public class QueryBuilder {
        private final String tableName;
        private String whereColumn;
        private String whereOperator = "";
        private Object whereValue;

        public QueryBuilder(String tableName) {
            this.tableName = tableName;
        }

        private Table getTable() {
            Table table = DBMS.this.getDictionary().getTable(this.tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table [" + this.tableName + "] does not exists");
            }
            return table;
        }

        public boolean insert(Object... values) {
            Table table = this.getTable();
            Column[] columns = table.getColumns();
            Record record = new Record(table);
            for (int i = 0; i < Math.min(table.getColumns().length, values.length); i++) {
                Column column = columns[i];
                switch (column.getType()) {
                    case CHAR -> record.setChar(column.getName(), (String) values[i]);
                    case VARCHAR -> record.setVarchar(column.getName(), (String) values[i]);
                }
            }
            return table.addRecord(record);
        }

        public QueryBuilder find(Object value) {
            return this.where(this.getTable().getPrimaryColumn(), value);
        }

        private QueryBuilder where(String column, String operator, Object value) {
            this.whereColumn = column;
            this.whereOperator = operator;
            this.whereValue = value;
            return this;
        }

        public QueryBuilder where(String column, Object value) {
            return this.where(column, "=", value);
        }

        public QueryBuilder whereNot(String column, Object value) {
            return this.where(column, "!=", value);
        }

        public QueryBuilder whereNull(String column) {
            return this.where(column, "IS", null);
        }

        public QueryBuilder whereNotNull(String column) {
            return this.where(column, "IS NOT", null);
        }

        public Record[] get() {
            Table table = this.getTable();

            return switch (this.whereOperator) {
                case "=", "IS" -> table.where(this.whereColumn, this.whereValue);
                case "!=", "IS NOT" -> table.whereNot(this.whereColumn, this.whereValue);
                default -> table.getAllRecords().toArray(Record[]::new);
            };
        }

        public Record[] getAndPrint() {
            Record[] records = this.get();
            Column[] columns = this.getTable().getColumns();

            // calculate column size
            HashMap<String, Integer> columnSize = new HashMap<>(columns.length);
            for (Column column : columns) {
                columnSize.put(column.getName(), column.getName().length());
            }
            for (Record record : records) {
                for (Column column : columns) {
                    String value = switch (column.getType()) {
                        case CHAR -> record.getChar(column.getName());
                        case VARCHAR -> record.getVarchar(column.getName());
                    };
                    String nullSafeValue = value == null ? "(null)" : value;
                    columnSize.computeIfPresent(column.getName(), (k, v) -> Math.max(v, nullSafeValue.length()));
                }
            }

            // print header
            for (Column column : columns) {
                System.out.print("+" + "-".repeat(columnSize.get(column.getName()) + 2));
            }
            System.out.println("+");
            for (Column column : columns) {
                System.out.format("| %-" + columnSize.get(column.getName()) + "s ", column.getName());
            }
            System.out.println("|");
            for (Column column : columns) {
                System.out.print("+" + "-".repeat(columnSize.get(column.getName()) + 2));
            }
            System.out.println("+");
            // print data
            for (Record record : records) {
                for (Column column : columns) {
                    String value = switch (column.getType()) {
                        case CHAR -> record.getChar(column.getName());
                        case VARCHAR -> record.getVarchar(column.getName());
                    };
                    String nullSafeValue = value == null ? "(null)" : value;
                    System.out.format("| %-" + columnSize.get(column.getName()) + "s ", nullSafeValue);
                }
                System.out.println("|");
            }
            for (Column column : columns) {
                System.out.print("+" + "-".repeat(columnSize.get(column.getName()) + 2));
            }
            System.out.println("+");

            return records;
        }

        public boolean delete() {
            Table table = this.getTable();

            return switch (this.whereOperator) {
                case "=", "IS" -> table.delete(this.whereColumn, this.whereValue) > 0;
                case "!=", "IS NOT" -> table.deleteNot(this.whereColumn, this.whereValue) > 0;
                default -> table.truncate();
            };
        }

        @Override
        public String toString() {
            StringBuilder query = new StringBuilder("SELECT * FROM ").append(this.tableName);
            if (!this.whereOperator.isEmpty()) {
                query.append(" WHERE ")
                        .append(this.whereColumn)
                        .append(" ").append(this.whereOperator).append(" ");
                if (this.whereValue == null) {
                    query.append("NULL");
                } else {
                    query.append("'").append(((String) this.whereValue).replace("'", "\\'")).append("'");
                }
            }
            return query.toString();
        }
    }

    @Override
    public void close() throws IOException {
        this.getBufferManager().close();
        this.getFilePool().close();
    }
}
