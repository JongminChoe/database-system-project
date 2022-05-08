package MyDBMS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPage {
    public static final int PAGE_SIZE = 4096;

    private final String fileName;
    private final long index;
    private final byte[] payload;
    private final ReadWriteLock lock;
    private boolean dirty;

    public BufferPage(String fileName, long index) throws IOException {
        this(fileName, index, new byte[PAGE_SIZE]);

        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        this.readFromFile(file);
        file.close();
    }

    public BufferPage(String fileName, long index, RandomAccessFile file) throws IOException {
        this(fileName, index, new byte[PAGE_SIZE]);

        this.readFromFile(file);
    }

    public BufferPage(String fileName, long index, byte[] payload) {
        assert payload.length == PAGE_SIZE;

        this.fileName = fileName;
        this.index = index;
        this.payload = payload;
        this.lock = new ReentrantReadWriteLock();
        this.dirty = true;
    }

    public void readFromFile(RandomAccessFile file) throws IOException {
        file.seek(this.index * PAGE_SIZE);
        if (file.read(this.payload) < 0) {
            throw new IndexOutOfBoundsException("End of file");
        }
        this.dirty = false;
    }

    public void flush() throws IOException {
        if (!this.isDirty()) {
            return;
        }

        this.writeLock();

        RandomAccessFile file = new RandomAccessFile(fileName, "rwd");
        file.seek(index * PAGE_SIZE);
        file.write(this.payload);
        file.close();

        this.writeUnlock();

        this.dirty = false;
    }

    public void readLock() {
        this.lock.readLock().lock();
    }

    public void readUnlock() {
        this.lock.readLock().unlock();
    }

    public void writeLock() {
        this.lock.writeLock().lock();
    }

    public void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public boolean isDirty() {
        return this.dirty;
    }

    public String getFileName() {
        return this.fileName;
    }

    public long getIndex() {
        return this.index;
    }

    public byte[] getPayload() {
        return this.payload;
    }
}
