package MyDBMS;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Record {
    private final Table table;
    private final Column[] columnBlueprints;
    private final HashMap<String, byte[]> columnData;

    public Record(Table table, byte[] payload) {
        this(table);
        this.parseBytes(payload);
    }

    public Record(Table table) {
        this.table = table;
        this.columnBlueprints = table.getColumns();
        this.columnData = new LinkedHashMap<>(columnBlueprints.length);
        for (Column columnBlueprint : this.columnBlueprints) {
            this.columnData.put(columnBlueprint.getName(), null);
        }
    }

    private void parseBytes(byte[] payload) {
        ByteBuffer stream = ByteBuffer.wrap(payload);

        for (Column columnBlueprint : this.columnBlueprints) {
            byte[] column;
            if (columnBlueprint.getType().isVariableLength()) {
                column = this.getReference(payload, stream.getShort(), stream.getShort());
            }
            else {
                column = new byte[columnBlueprint.getSize()];
                stream.get(column);
            }
            this.columnData.put(columnBlueprint.getName(), column);
        }

        byte[] nullBytes = new byte[this.getNullBitmapLength()];
        stream.get(nullBytes);
        Bitmap nullBitmap = new Bitmap(nullBytes);

        for (int columnIndex = 0; columnIndex < this.columnBlueprints.length; columnIndex++) {
            if (nullBitmap.get(columnIndex)) {
                this.columnData.put(this.columnBlueprints[columnIndex].getName(), null);
            }
        }
    }

    private byte[] getReference(byte[] payload, int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(payload, offset, result, 0, result.length);
        return result;
    }

    private int getNullBitmapLength() {
        return (this.columnBlueprints.length + 7) / 8;
    }

    public byte[] toByteArray() {
        ByteBuffer stream = ByteBuffer.allocate(4090);

        int position = 0;
        for (Column columnBlueprint : this.columnBlueprints) {
            if (columnBlueprint.getType().isVariableLength()) {
                position += 4;
            }
            else {
                position += columnBlueprint.getSize();
            }
        }
        position += this.getNullBitmapLength();

        Bitmap nullBitmap = new Bitmap(this.columnBlueprints.length);

        for (int columnIndex = 0; columnIndex < this.columnBlueprints.length; columnIndex++) {
            Column columnBlueprint = this.columnBlueprints[columnIndex];
            byte[] data = this.columnData.get(columnBlueprint.getName());

            if (columnBlueprint.getType().isVariableLength()) {
                int length = data == null ? 0 : data.length;
                stream.putShort((short) position);
                stream.putShort((short) length);
                position += length;
            }
            else {
                stream.put(data == null ? new byte[columnBlueprint.getSize()] : data);
            }

            if (data == null) {
                nullBitmap.set(columnIndex);
            }
        }
        stream.put(nullBitmap.toByteArray());

        for (Column columnBlueprint : this.columnBlueprints) {
            if (columnBlueprint.getType().isVariableLength()) {
                byte[] data = this.columnData.get(columnBlueprint.getName());
                if (data != null) {
                    stream.put(data);
                }
            }
        }

        byte[] result = new byte[position];
        stream.get(0, result);
        return result;
    }

    public String getTableName() {
        return this.table.getTableName();
    }

    public String getChar(String columnName) {
        Column columnBlueprint = this.getColumnBlueprint(columnName);
        if (columnBlueprint == null) {
            throw new IllegalArgumentException("Column [" + columnName + "] does not exists");
        }
        if (columnBlueprint.getType() != Column.DataType.CHAR) {
            throw new IllegalArgumentException("Column [" + columnName + "] is not CHAR type");
        }
        byte[] column = this.columnData.get(columnName);
        return column == null ? null : new String(column);
    }

    public String getVarchar(String columnName) {
        Column columnBlueprint = this.getColumnBlueprint(columnName);
        if (columnBlueprint == null) {
            throw new IllegalArgumentException("Column [" + columnName + "] does not exists");
        }
        if (columnBlueprint.getType() != Column.DataType.VARCHAR) {
            throw new IllegalArgumentException("Column [" + columnName + "] is not VARCHAR type");
        }
        byte[] column = this.columnData.get(columnName);
        return column == null ? null : new String(column);
    }

    private Column getColumnBlueprint(String columnName) {
        Column columnBlueprint = null;
        for (Column column : this.columnBlueprints) {
            if (column.getName().equals(columnName)) {
                columnBlueprint = column;
                break;
            }
        }
        return columnBlueprint;
    }

    public Record setChar(String columnName, String value) {
        Column columnBlueprint = this.getColumnBlueprint(columnName);
        if (columnBlueprint == null) {
            throw new IllegalArgumentException("Column [" + columnName + "] does not exists");
        }
        if (columnBlueprint.getType() != Column.DataType.CHAR) {
            throw new IllegalArgumentException("Column [" + columnName + "] is not CHAR type");
        }
        if (value == null && columnBlueprint.isNotnull()) {
            throw new IllegalArgumentException("Column [" + columnName + "] cannot be null");
        }

        byte[] data = null;
        if (value != null) {
            data = new byte[columnBlueprint.getSize()];
            Arrays.fill(data, (byte) ' ');
            byte[] byteValue = value.getBytes();
            System.arraycopy(byteValue, 0, data, 0, Math.min(byteValue.length, data.length));
        }

        this.columnData.put(columnBlueprint.getName(), data);
        return this;
    }

    public Record setVarchar(String columnName, String value) {
        Column columnBlueprint = this.getColumnBlueprint(columnName);
        if (columnBlueprint == null) {
            throw new IllegalArgumentException("Column [" + columnName + "] does not exists");
        }
        if (columnBlueprint.getType() != Column.DataType.VARCHAR) {
            throw new IllegalArgumentException("Column [" + columnName + "] is not VARCHAR type");
        }
        if (value == null && columnBlueprint.isNotnull()) {
            throw new IllegalArgumentException("Column [" + columnName + "] cannot be null");
        }

        byte[] data = null;
        if (value != null) {
            data = new byte[Math.min(value.getBytes().length, columnBlueprint.getSize())];
            System.arraycopy(value.getBytes(), 0, data, 0, data.length);
        }

        this.columnData.put(columnBlueprint.getName(), data);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record record)) return false;
        if (!this.table.equals(record.table)) return false;
        if (!this.columnData.keySet().equals(record.columnData.keySet())) return false;
        for (String key : this.columnData.keySet()) {
            if (!Arrays.equals(this.columnData.get(key), record.columnData.get(key))) return false;
        }
        return true;
    }
}
