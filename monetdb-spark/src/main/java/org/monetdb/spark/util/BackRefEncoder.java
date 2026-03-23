package org.monetdb.spark.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BackRefEncoder {
    private final byte[] NILREPR = { -0x80, 0 };
    private final ByteBuffer encoded = ByteBuffer.allocate(16);
    private long itemCounter = 1; // valid values are > 0
    private final long[] lastShort = new long[129]; // pos 0 is used for empty, 128 for null
    private Cache currentCache;
    private Cache previousCache;

    /**
     * Construct a BackRefEncoder.
     * @param withCache if true, the backref encoder tracks multi-character strings.
     */
    public BackRefEncoder(boolean multiChar) {
        if (multiChar) {
            currentCache = new Cache();
            previousCache = new Cache();
        }
    }

    public BackRefEncoder() {
        this(true);
    }

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
            return examineShort(128, itemNr);
        } else if (item.limit() == 0) {
            return examineShort(0, itemNr);
        } else if (item.position() == 0 && item.limit() == 1 && item.get(0) >= 0) {
            return examineShort(item.get(0), itemNr);
        } else {
            return examineLong(item, itemNr);
        }
    }

    private boolean examineShort(int b, long itemNr) {
        if (lastShort[b] == 0) {
            lastShort[b] = itemNr;
            return false;
        }
        // Remember we saw it and compute the delta
        long delta = itemNr - lastShort[b];
        lastShort[b] = itemNr;
        return encode(delta);
    }


    private boolean examineLong(ByteBuffer item, long itemNr) {
        if (currentCache == null)
            return false;
        if (item.remaining() > currentCache.MAX_ITEM_SIZE)
            return false;
        int hash = item.hashCode();

        // First look it up in the current cache. If we have it we're done
        long earlier;
        earlier = currentCache.lookup(item, hash, itemNr);
        if (earlier > 0) {
            return encode(itemNr - earlier);
        } else {
            // Maybe the previous cache has it. In that case we can use that number.
            // Next time we'll find it in our current cache.
            earlier = previousCache.lookup(item, hash, itemNr);
        }

        // Before we insert we may have to make room
        if (!currentCache.fits(item)) {
            Cache tmp = previousCache;
            previousCache = currentCache;
            currentCache = tmp;
            currentCache.reset();
        }
        currentCache.append(item, hash, itemNr);

        // 'earlier' has been -1 but may have been overwritten by the second cache
        if (earlier > 0)
            return encode(itemNr - earlier);
        else
            return false;
    }

    private boolean encode(long delta) {
        encoded.clear();
        if (delta < 64) {
            encoded.put((byte)(0x80 + delta));
        } else {
            encoded.put((byte) 0x80);
            while (delta > 0) {
                long chunk = delta % 128;
                if (chunk < delta) {
                    // not last
                    chunk += 0x80;
                }
                encoded.put((byte) chunk);
                delta /= 128;
            }
        }
        encoded.flip();
        return true;
    }

    public ByteBuffer encoded() {
        return encoded;
    }

    private class Cache {
        private final int SIZE = 2 << 20;
        private final int MAX_ITEM_SIZE = SIZE / 3;
        private final int NSLOTS = 1 << 16;
        private final int[] offsets;
        private final long[] itemNumbers;
        private final ByteBuffer text;

        private Cache() {
            offsets = new int[NSLOTS];
            itemNumbers = new long[NSLOTS];
            text = ByteBuffer.allocate(SIZE);
            text.position(1); // 0 reserved for 'not present'
        }

        void reset() {
            text.position(1); // 0 reserved for 'not present'
            for (int i = 0; i < NSLOTS; i++)
                offsets[i] = 0;  // not present anymore
        }

        /**
         * Find the item in the cache and return its item number.
         * Also update the item number to the newer value 'itemNr'
         */
        long lookup(ByteBuffer s, int hash, long itemNr) {
            int index = hashToIndex(hash);
            int offset = offsets[index];
            if (offset == 0) {
                // not present
                return -1;
            }
            int len = s.remaining();
            if (text.position() - offset < len + 1) {
                // s is too long, this positino can't be a match
                return -1;
            }
            if (text.get(offset + len) != 0) {
                // we expect a trailing NUL byte there
                return -1;
            }
            if (s.compareTo(text.slice(offset, len)) != 0) {
                // text doesn't match
                return -1;
            }

            // We have found a previous occurrence.
            // Return it but from now on remember the more current one.
            long previous = itemNumbers[index];
            itemNumbers[index] = itemNr;
            return previous;
        }

        boolean fits(ByteBuffer s) {
            return text.remaining() >= s.remaining() + 1;
        }

        void append(ByteBuffer s, int hash, long itemNr) {
            int index = hashToIndex(hash);
            offsets[index] = text.position();
            itemNumbers[index] = itemNr;
            text.put(s.duplicate());
            text.put((byte)0);
        }

        private int hashToIndex(int hash) {
            // Hash can be negative!
            long neverNegative = (1L<<32) + hash;
            return (int)(neverNegative % NSLOTS);
        }
    }
}
