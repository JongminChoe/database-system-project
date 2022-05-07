package MyDBMS;

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

    public Column(DataType type, String name, int size) {
        this.type = type;
        this.name = name;
        this.size = size;
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
}
