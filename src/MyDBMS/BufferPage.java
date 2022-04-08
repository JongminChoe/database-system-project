package MyDBMS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPage {
    public static final int PAGE_SIZE = 4096;

    private String fileName;
    private long index;
    private byte[] payload;
    private ReadWriteLock lock;
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
        this.fileName = fileName;
        this.index = index;
        this.payload = payload;
        this.lock = new ReentrantReadWriteLock();
        this.dirty = false;
    }

    public void readFromFile(RandomAccessFile file) throws IOException {
        file.seek(this.index * PAGE_SIZE);
        file.read(this.payload);
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
        return dirty;
    }
}
