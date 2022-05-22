package MyDBMS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
                (prev, next) -> next,
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

        return this.getAllRecords().filter(record -> record.getChar(column).equals(value)).toArray(Record[]::new);
    }

    public Record[] whereVarchar(String column, String value) {
        if (!this.columns.containsKey(column)) {
            throw new IllegalArgumentException("Column [" + column + "] does not exists");
        }
        if (this.columns.get(column).getType() != Column.DataType.VARCHAR) {
            throw new IllegalArgumentException("Column [" + column + "] is not CHAR type");
        }

        return this.getAllRecords().filter(record -> record.getVarchar(column).equals(value)).toArray(Record[]::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Table table)) return false;
        return Objects.equals(this.getTableName(), table.getTableName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getTableName());
    }
}
