package org.monetdb.spark.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BackRefEncoder {
    private final byte[] NILREPR = { -0x80, 0 };
    private final ByteBuffer encoded = ByteBuffer.allocate(16);
    private long itemCounter = 1; // valid values are > 0
    private final long[] lastShort = new long[129]; // pos 0 is used for empty, 128 for null

    /**
     * Write the item to the stream, hopefully backref encoded.
     */
    public void write(ByteBuffer item, OutputStream out) throws IOException {
        boolean found = examine(item);
        if (found) {
            ByteBuffer value = encoded();
            int start = value.arrayOffset() + value.position();
            int len = value.limit() - value.position();
            out.write(value.array(), start, len);
        } else if (item != null) {
            int start = item.arrayOffset() + item.position();
            int len = item.limit() - item.position();
            out.write(item.array(), start, len);
            out.write(0);
        } else {
            out.write(NILREPR);
        }
    }

    /**
     * Return whether the item has been seem before. If so,
     * {@link #encoded()} can be used to retrieve the encoding.
     * @param item
     * @return
     */
    public boolean examine(ByteBuffer item) {
        long itemNr = itemCounter++;

        if (item == null) {
            return recordShort(128, itemNr);
        } else if (item.limit() == 0) {
            return recordShort(0, itemNr);
        } else if (item.position() == 0 && item.limit() == 1 && item.get(0) >= 0) {
            return recordShort(item.get(0), itemNr);
        } else {
            return false;
        }
    }

    private boolean recordShort(int b, long itemNr) {
        if (lastShort[b] == 0) {
            lastShort[b] = itemNr;
            return false;
        }
        // Remember we saw it and compute the delta
        long delta = itemNr - lastShort[b];
        lastShort[b] = itemNr;

        encode(delta);
        return true;
    }

    private void encode(long delta) {
        encoded.clear();
        if (delta < 64) {
            encoded.put((byte)(0x80 + delta));
            encoded.flip();
            return;
        }
        encoded.put((byte)0x80);
        while (delta > 0) {
            long chunk = delta % 128;
            if (chunk < delta)
                delta += 0x80;
            encoded.put((byte) chunk);
            delta /= 128;
        }
        encoded.flip();
    }

    public ByteBuffer encoded() {
        return encoded;
    }
}
