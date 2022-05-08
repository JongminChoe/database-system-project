package MyDBMS;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Stack;

public class BufferManager {
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

    public BufferPage get(String key, long index) throws IOException {
        int indexInBuffer = this.search(key, index);
        if (indexInBuffer >= 0) {
            return this.buffer.get(indexInBuffer);
        }

        // page not found in buffer
        BufferPage page = new BufferPage(key, index);

        if (buffer.size() >= BUFFER_CAPACITY) {
            // MRU
            buffer.pop().flush();
        }

        buffer.push(page);

        return page;
    }

    public int search(String key, long index) {
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

    public void flush(String key, long index) throws IOException {
        int indexInBuffer = this.search(key, index);
        if (indexInBuffer < 0) {
            return;
        }
        this.buffer.remove(indexInBuffer).flush();
    }

    public void flush(String key) throws IOException {
        for (BufferPage page : this.buffer) {
            if (page.getFileName().equals(key)) {
                page.flush();
            }
        }
        this.buffer.removeIf(page -> page.getFileName().equals(key));
    }

    public void flush() throws IOException {
        for (BufferPage page : this.buffer) {
            page.flush();
        }
        this.buffer.clear();
    }

    public void forceFlush() {
        this.buffer.clear();
    }
}
