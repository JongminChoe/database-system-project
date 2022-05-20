package MyDBMS;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPage {
    public static final int PAGE_SIZE = 4096;

    private final String fileName;
    private final long index;
    private final byte[] payload;
    private final ReadWriteLock lock;
    private boolean dirty;

    public BufferPage(String fileName, long index) {
        this(fileName, index, new byte[PAGE_SIZE]);
    }

    public BufferPage(String fileName, long index, byte[] payload) {
        this(fileName, index, payload, false);
    }

    public BufferPage(String fileName, long index, byte[] payload, boolean dirty) {
        assert payload.length == PAGE_SIZE;

        this.fileName = fileName;
        this.index = index;
        this.payload = payload;
        this.lock = new ReentrantReadWriteLock();
        this.dirty = dirty;
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
