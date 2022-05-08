package MyDBMS;

import java.util.Objects;

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
