package MyDBMS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SlottedPage {
    private final Table table;
    private int numberOfEntries;
    private final List<Record> entries;

    public SlottedPage(Table table, byte[] payload) {
        this(table);
        this.parseBytes(payload);
    }

    public SlottedPage(Table table) {
        this.table = table;
        this.numberOfEntries = 0;
        this.entries = new ArrayList<>();
    }

    private void parseBytes(byte[] payload) {
        ByteBuffer stream = ByteBuffer.wrap(payload);

        this.numberOfEntries = stream.getShort();

        for (int i = 0; i < numberOfEntries; i++) {
            int recordLength = stream.getShort();
            int recordStartOffset = stream.getShort();

            Record record = null;
            if (recordLength > 0) {
                byte[] recordBytes = new byte[recordLength];
                stream.get(recordBytes, recordStartOffset, recordBytes.length);
                record = new Record(this.table, recordBytes);
            }
            this.entries.add(record);
        }
    }

    public int getFreeSpaceSize() {
        return BufferPage.PAGE_SIZE
                - 2
                - this.numberOfEntries * 4
                - this.getRecords().stream().mapToInt(record -> record.toByteArray().length).sum();
    }

    public void addRecord(Record record) {
        if (!this.table.getTableName().equals(record.getTableName())) {
            throw new IllegalArgumentException("Table mismatch");
        }
        if (this.getFreeSpaceSize() < record.toByteArray().length) {
            throw new IndexOutOfBoundsException("Page is full");
        }
        this.numberOfEntries++;
        this.entries.add(record);
    }

    public int getNumberOfEntries() {
        return this.numberOfEntries;
    }

    public List<Record> getRecords() {
        return this.entries.stream().filter(Objects::nonNull).toList();
    }

    public byte[] toByteArray() {
        ByteBuffer stream = ByteBuffer.allocate(BufferPage.PAGE_SIZE);
        int freeSpaceEndOffset = BufferPage.PAGE_SIZE;

        stream.putShort((short) this.numberOfEntries);
        for (Record record : this.entries) {
            byte[] recordBytes = record == null ? new byte[0] : record.toByteArray();
            stream.putShort((short) recordBytes.length);
            freeSpaceEndOffset -= recordBytes.length;
            stream.putShort((short) freeSpaceEndOffset);
            stream.put(freeSpaceEndOffset, recordBytes);
        }

        return stream.array();
    }
}
