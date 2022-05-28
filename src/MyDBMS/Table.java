package MyDBMS;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Table {
    private final String tableName;
    private final HashMap<String, Column> columns;
    private final String primaryColumn;

    public Table(String tableName, Column[] columns) {
        this(tableName, columns, null);
    }

    public Table(String tableName, Column[] columns, String primaryColumn) {
        this.tableName = tableName;
        this.columns = Arrays.stream(columns).collect(Collectors.toMap(
                Column::getName,
                Function.identity(),
                (prev, next) -> {
                    if (prev.getName().equals(next.getName())) {
                        throw new IllegalStateException(String.format(
                                "Duplicate key %s (attempted merging values %s and %s)",
                                prev.getName(), prev, next));
                    }
                    return next;
                },
                LinkedHashMap::new
        ));
        if (primaryColumn != null && !this.columns.containsKey(primaryColumn)) {
            throw new IllegalArgumentException("primary column does not exists in columns");
        }
        this.primaryColumn = primaryColumn;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Column[] getColumns() {
        return this.columns.values().toArray(Column[]::new);
    }

    public String getPrimaryColumn() {
        return this.primaryColumn;
    }

    public Stream<Record> getAllRecords() {
        return Stream
                .iterate(0, n -> n + 1)
                .map(i -> {
                    try {
                        return DBMS.getInstance().getBufferManager().getPage(this.getTableName(), i);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .takeWhile(Objects::nonNull)
                .flatMap(page -> new SlottedPage(this, page.getPayload()).getRecords().stream());
    }

    public boolean addRecord(Record record) {
        if (this.getPrimaryColumn() != null) {
            Object primaryKey = switch (this.columns.get(this.getPrimaryColumn()).getType()) {
                case CHAR -> record.getChar(this.getPrimaryColumn());
                case VARCHAR -> record.getVarchar(this.getPrimaryColumn());
            };
            if (this.find(primaryKey) != null) {
                return false;
            }
        }

        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = DBMS.getInstance().getBufferManager().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                try {
                    bufferPage = DBMS.getInstance().getBufferManager().getEmptyPage(this.getTableName(), i);
                } catch (IOException ex) {
                    // unknown error
                    return false;
                }
            }
            SlottedPage slottedPage = new SlottedPage(this, bufferPage.getPayload());
            try {
                slottedPage.addRecord(record);
            } catch (Exception e) {
                // page full
                continue;
            }
            bufferPage.setPayload(slottedPage.toByteArray());
            return true;
        }
    }

    public Record find(Object value) {
        if (this.getPrimaryColumn() == null) {
            return null;
        }

        Record[] records = this.where(this.getPrimaryColumn(), value);

        return records.length > 0 ? records[0] : null;
    }

    public Record[] where(String column, Object value) {
        return this.where(column, value, false);
    }

    public Record[] whereNot(String column, Object value) {
        return this.where(column, value, true);
    }

    private Record[] where(String column, Object value, boolean not) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }

        return switch (this.columns.get(column).getType()) {
            case CHAR -> this.getAllRecords().filter(record -> not ^ Objects.equals(record.getChar(column), value)).toArray(Record[]::new);
            case VARCHAR -> this.getAllRecords().filter(record -> not ^ Objects.equals(record.getVarchar(column), value)).toArray(Record[]::new);
        };
    }

    public boolean destroy(Object primaryKey) {
        if (this.getPrimaryColumn() == null) {
            return false;
        }

        return this.delete(this.getPrimaryColumn(), primaryKey) > 0;
    }

    public int delete(String column, Object value) {
        return this.delete(column, value, false);
    }

    public int deleteNot(String column, Object value) {
        return this.delete(column, value, true);
    }

    private int delete(String column, Object value, boolean not) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }

        int deleted = 0;
        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = DBMS.getInstance().getBufferManager().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                return deleted;
            }
            SlottedPage slottedPage = new SlottedPage(this, bufferPage.getPayload());
            for (Record record : slottedPage.getRecords()) {
                Object columnValue = switch (this.columns.get(column).getType()) {
                    case CHAR -> record.getChar(column);
                    case VARCHAR -> record.getVarchar(column);
                };
                if (not ^ Objects.equals(columnValue, value)) {
                    slottedPage.removeRecord(record);
                    bufferPage.setPayload(slottedPage.toByteArray());
                    deleted++;
                }
            }
        }
    }

    public boolean truncate() {
        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = DBMS.getInstance().getBufferManager().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                return true;
            }
            SlottedPage slottedPage = new SlottedPage(this, bufferPage.getPayload());
            slottedPage.removeAll();
            bufferPage.setPayload(slottedPage.toByteArray());
        }
    }

    public void flush() throws IOException {
        DBMS.getInstance().getBufferManager().flush(this.getTableName());
    }

    public void forceFlush() {
        DBMS.getInstance().getBufferManager().forceFlush(this.getTableName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Table table)) return false;
        return this.getTableName().equals(table.getTableName())
                && new HashSet<>(Arrays.asList(this.getColumns())).equals(new HashSet<>(Arrays.asList(table.getColumns())))
                && Objects.equals(this.getPrimaryColumn(), table.getPrimaryColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getTableName(), new HashSet<>(Arrays.asList(this.getColumns())), this.getPrimaryColumn());
    }
}
