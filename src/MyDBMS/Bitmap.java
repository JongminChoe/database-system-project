package MyDBMS;

class Bitmap {
    private final byte[] data;

    public Bitmap(int size) {
        this.data = new byte[(size + 7) / 8];
    }

    public Bitmap(byte[] data) {
        this.data = data;
    }

    public void set(int index) {
        this.set(index, true);
    }

    public void set(int index, boolean value) {
        if (value) {
            this.data[index / 8] |= 1 << (7 - index % 8);
        } else {
            this.data[index / 8] &= ~(1 << (7 - index % 8));
        }
    }

    public boolean get(int index) {
        return (this.data[index / 8] & (1 << (7 - index % 8))) > 0;
    }

    public byte[] getByteArray() {
        return this.data;
    }
}
