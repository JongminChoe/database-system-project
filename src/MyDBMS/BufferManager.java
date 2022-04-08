package MyDBMS;

import java.io.IOException;
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
        for (BufferPage page : this.buffer) {
            if (page.getFileName().equals(key) && page.getIndex() == index) {
                return page;
            }
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

    public void flush(String key, long index) throws IOException {
        for (BufferPage page : this.buffer) {
            if (page.getFileName().equals(key) && page.getIndex() == index) {
                page.flush();
                return;
            }
        }
    }

    public void flush(String key) throws IOException {
        for (BufferPage page : this.buffer) {
            if (page.getFileName().equals(key)) {
                page.flush();
            }
        }
    }

    public void flush() throws IOException {
        for (BufferPage page : this.buffer) {
            page.flush();
        }
    }
}
