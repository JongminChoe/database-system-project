package MyDBMS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

public class FilePool {
    public static final int MAX_OPEN_FILE = 10;

    private final LinkedHashMap<String, RandomAccessFile> pool;

    private FilePool() {
        this.pool = new LinkedHashMap<>(MAX_OPEN_FILE);
    }

    public static FilePool getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final FilePool INSTANCE = new FilePool();
    }

    public byte[] read(String fileName, long offset, int length) throws IOException {
        byte[] payload = new byte[length];
        RandomAccessFile file = this.getFile(fileName);
        file.seek(offset);
        file.read(payload);
        return payload;
    }

    public void write(String fileName, long offset, byte[] payload) throws IOException {
        RandomAccessFile file = this.getFile(fileName);
        file.seek(offset);
        file.write(payload);
    }

    public void delete(String fileName) throws IOException {
        if (this.pool.containsKey(fileName)) {
            this.pool.remove(fileName).close();
        }
        Path path = Path.of(fileName);
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    public int size() {
        return this.pool.size();
    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile file = this.pool.remove(fileName);
        if (file == null) {
            if (this.pool.size() >= MAX_OPEN_FILE) {
                this.pool.remove(this.pool.keySet().iterator().next()).close();
            }
            file = new RandomAccessFile(fileName, "rwd");
        }
        this.pool.put(fileName, file);
        return file;
    }
}
