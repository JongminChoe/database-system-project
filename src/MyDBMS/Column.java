package MyDBMS;

import java.util.Objects;

public class Column {

    public enum DataType {
        CHAR("CHAR", false), VARCHAR("VARCHAR", true);

        private final String value;
        private final boolean variableLength;

        DataType(String value, boolean variableLength) {
            this.value = value;
            this.variableLength = variableLength;
        }

        public String getValue() {
            return this.value;
        }

        public boolean isVariableLength() {
            return this.variableLength;
        }
    }

    private final DataType type;
    private final String name;
    private final int size;
    private final boolean notnull;

    public Column(DataType type, String name, int size) {
        this(type, name, size, false);
    }

    public Column(DataType type, String name, int size, boolean notnull) {
        this.type = type;
        this.name = name;
        this.size = size;
        this.notnull = notnull;
    }

    public DataType getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public int getSize() {
        return this.size;
    }

    public boolean isNotnull() {
        return this.notnull;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column column)) return false;
        return getSize() == column.getSize() && isNotnull() == column.isNotnull() && getType() == column.getType() && getName().equals(column.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getName(), getSize(), isNotnull());
    }
}
