package MyDBMS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
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
                HashMap::new
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
                        return BufferManager.getInstance().getPage(this.getTableName(), i);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .takeWhile(Objects::nonNull)
                .flatMap(page -> new SlottedPage(this, page.getPayload()).getRecords().stream());
    }

    public void addRecord(Record record) {
        if (this.getPrimaryColumn() != null) {
            Object primaryKey = switch (this.columns.get(this.getPrimaryColumn()).getType()) {
                case CHAR -> record.getChar(this.getPrimaryColumn());
                case VARCHAR -> record.getVarchar(this.getPrimaryColumn());
            };
            if (this.find(primaryKey) != null) {
                return;
            }
        }

        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = BufferManager.getInstance().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                try {
                    bufferPage = BufferManager.getInstance().getEmptyPage(this.getTableName(), i);
                } catch (IOException ex) {
                    // unknown error
                    break;
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
            break;
        }
    }

    public Record find(Object value) {
        if (this.getPrimaryColumn() == null) {
            return null;
        }

        Record[] records = switch (this.columns.get(this.getPrimaryColumn()).getType()) {
            case CHAR -> this.whereChar(this.getPrimaryColumn(), (String) value);
            case VARCHAR -> this.whereVarchar(this.getPrimaryColumn(), (String) value);
        };

        return records.length > 0 ? records[0] : null;
    }

    public Record[] whereChar(String column, String value) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }
        if (this.columns.get(column).getType() != Column.DataType.CHAR) {
            throw new IllegalArgumentException("Column [" + column + "] is not CHAR type");
        }

        return this.getAllRecords().filter(record -> Objects.equals(record.getChar(column), value)).toArray(Record[]::new);
    }

    public Record[] whereVarchar(String column, String value) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }
        if (this.columns.get(column).getType() != Column.DataType.VARCHAR) {
            throw new IllegalArgumentException("Column [" + column + "] is not CHAR type");
        }

        return this.getAllRecords().filter(record -> Objects.equals(record.getVarchar(column), value)).toArray(Record[]::new);
    }

    public boolean destroy(Object primaryKey) {
        if (this.getPrimaryColumn() == null) {
            return false;
        }

        return switch (this.columns.get(this.getPrimaryColumn()).getType()) {
            case CHAR -> this.deleteWhereChar(this.getPrimaryColumn(), (String) primaryKey);
            case VARCHAR -> this.deleteWhereVarchar(this.getPrimaryColumn(), (String) primaryKey);
        };
    }

    public boolean deleteWhereChar(String column, String value) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }
        if (this.columns.get(column).getType() != Column.DataType.CHAR) {
            throw new IllegalArgumentException("Column [" + column + "] is not CHAR type");
        }

        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = BufferManager.getInstance().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                break;
            }
            SlottedPage slottedPage = new SlottedPage(this, bufferPage.getPayload());
            for (Record record : slottedPage.getRecords()) {
                if (Objects.equals(record.getChar(column), value)) {
                    slottedPage.removeRecord(record);
                    bufferPage.setPayload(slottedPage.toByteArray());
                    return true;
                }
            }
        }
        return false;
    }

    public boolean deleteWhereVarchar(String column, String value) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }
        if (this.columns.get(column).getType() != Column.DataType.VARCHAR) {
            throw new IllegalArgumentException("Column [" + column + "] is not VARCHAR type");
        }

        for (int i = 0; ; i++) {
            BufferPage bufferPage;
            try {
                bufferPage = BufferManager.getInstance().getPage(this.getTableName(), i);
            } catch (IOException e) {
                // end of file
                break;
            }
            SlottedPage slottedPage = new SlottedPage(this, bufferPage.getPayload());
            for (Record record : slottedPage.getRecords()) {
                if (Objects.equals(record.getVarchar(column), value)) {
                    slottedPage.removeRecord(record);
                    bufferPage.setPayload(slottedPage.toByteArray());
                    return true;
                }
            }
        }
        return false;
    }

    public void flush() throws IOException {
        BufferManager.getInstance().flush(this.getTableName());
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
