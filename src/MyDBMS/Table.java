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
