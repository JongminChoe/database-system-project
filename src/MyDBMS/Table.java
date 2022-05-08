package MyDBMS;

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
}
