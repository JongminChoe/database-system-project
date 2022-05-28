package MyDBMS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ListIterator;
import java.util.Stack;

public class BufferManager implements Closeable {
    public static final int BUFFER_CAPACITY = 100;

    public Stack<BufferPage> buffer;

    private BufferManager() {
        this.buffer = new Stack<>();
    }

    public static BufferManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final BufferManager INSTANCE = new BufferManager();
    }

    public BufferPage getPage(String key, long index) throws IOException {
        int indexInBuffer = this.search(key, index);
        if (indexInBuffer >= 0) {
            return this.buffer.get(indexInBuffer);
        }

        // page not found in buffer

        if (buffer.size() >= BUFFER_CAPACITY) {
            // MRU
            this.flushPage(this.buffer.pop());
        }

        byte[] payload = FilePool.getInstance().read(key, index * BufferPage.PAGE_SIZE, BufferPage.PAGE_SIZE);
        BufferPage page = new BufferPage(key, index, payload);

        buffer.push(page);

        return page;
    }

    public BufferPage getEmptyPage(String key, long index) throws IOException {
        int indexInBuffer = this.search(key, index);
        if (indexInBuffer >= 0) {
            return this.buffer.get(indexInBuffer);
        }

        // page not found in buffer

        if (buffer.size() >= BUFFER_CAPACITY) {
            // MRU
            this.flushPage(this.buffer.pop());
        }

        BufferPage page = new BufferPage(key, index);

        buffer.push(page);

        return page;
    }

    private int search(String key, long index) {
        ListIterator<BufferPage> iterator = this.buffer.listIterator();
        while (iterator.hasNext()) {
            int indexInBuffer = iterator.nextIndex();
            BufferPage page = iterator.next();
            if (page.getFileName().equals(key) && page.getIndex() == index) {
                return indexInBuffer;
            }
        }
        return -1;
    }

    public boolean contains(String key, long index) {
        return this.search(key, index) != -1;
    }

    public int getSize() {
        return this.buffer.size();
    }

    public void flush(String key, long index) throws IOException {
        int indexInBuffer = this.search(key, index);
        if (indexInBuffer < 0) {
            return;
        }
        this.flushPage(this.buffer.remove(indexInBuffer));
    }

    public void flush(String key) throws IOException {
        for (BufferPage page : this.buffer) {
            if (page.getFileName().equals(key)) {
                this.flushPage(page);
            }
        }
        this.forceFlush(key);
    }

    public void flush() throws IOException {
        for (BufferPage page : this.buffer) {
            this.flushPage(page);
        }
        this.forceFlush();
    }

    private void flushPage(BufferPage page) throws IOException {
        FilePool.getInstance().write(page.getFileName(), page.getOffset(), page.getPayload());
    }

    public void forceFlush(String key) {
        this.buffer.removeIf(page -> page.getFileName().equals(key));
    }

    public void forceFlush() {
        this.buffer.clear();
    }

    @Override
    public void close() throws IOException {
        this.flush();
    }
}
