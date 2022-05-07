package MyDBMS;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BitmapTest {

    @Test
    void testConstructorWithSize() {
        assertArrayEquals(new byte[0], new Bitmap(0).getByteArray());

        assertArrayEquals(new byte[1], new Bitmap(1).getByteArray());
        assertArrayEquals(new byte[1], new Bitmap(8).getByteArray());

        assertArrayEquals(new byte[2], new Bitmap(9).getByteArray());
    }

    @Test
    void testConstructorWithData() {
        assertArrayEquals(new byte[]{ 0, 1 }, new Bitmap(new byte[]{ 0, 1 }).getByteArray());
    }

    @Test
    void testGetAndSet1() {
        Bitmap bitmap = new Bitmap(1);

        bitmap.set(0);
        assertTrue(bitmap.get(0));

        assertFalse(bitmap.get(1));
        assertFalse(bitmap.get(2));
        assertFalse(bitmap.get(3));
        assertFalse(bitmap.get(4));
        assertFalse(bitmap.get(5));
        assertFalse(bitmap.get(6));
        assertFalse(bitmap.get(7));
    }

    @Test
    void testGetAndSet2() {
        Bitmap bitmap = new Bitmap(16);

        bitmap.set(0);
        bitmap.set(15);
        assertTrue(bitmap.get(0));
        assertTrue(bitmap.get(15));

        assertFalse(bitmap.get(1));
        assertFalse(bitmap.get(2));
        assertFalse(bitmap.get(3));
        assertFalse(bitmap.get(4));
        assertFalse(bitmap.get(5));
        assertFalse(bitmap.get(6));
        assertFalse(bitmap.get(7));
        assertFalse(bitmap.get(8));
        assertFalse(bitmap.get(9));
        assertFalse(bitmap.get(10));
        assertFalse(bitmap.get(11));
        assertFalse(bitmap.get(12));
        assertFalse(bitmap.get(13));
        assertFalse(bitmap.get(14));
    }

    @Test
    void testReset() {
        Bitmap bitmap = new Bitmap(1);

        bitmap.set(0);
        assertTrue(bitmap.get(0));
        bitmap.set(0, false);
        assertFalse(bitmap.get(0));

        assertFalse(bitmap.get(1));
        assertFalse(bitmap.get(2));
        assertFalse(bitmap.get(3));
        assertFalse(bitmap.get(4));
        assertFalse(bitmap.get(5));
        assertFalse(bitmap.get(6));
        assertFalse(bitmap.get(7));
    }

    @Test
    void testGetByteArray() {
        Bitmap bitmap = new Bitmap(1);

        bitmap.set(0);
        assertArrayEquals(new byte[]{(byte) 128}, bitmap.getByteArray());
    }

    @Test
    void testOutOfBound() {
        Bitmap bitmap = new Bitmap(0);

        assertThrows(IndexOutOfBoundsException.class, () -> bitmap.set(1));
        assertThrows(IndexOutOfBoundsException.class, () -> bitmap.get(1));
    }
}