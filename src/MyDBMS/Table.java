package MyDBMS;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

public class Table {
    private final String tableName;
    private final Column[] columns;

    public Table(String tableName, Column[] columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Column[] getColumns() {
        return this.columns;
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
