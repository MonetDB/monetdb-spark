package org.monetdb.spark.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BackRefEncoder {
    public static final int MIN_CACHE_SIZE = 8192;
    public static final int MAX_CACHE_SIZE = 1 << 24;
    private final byte[] NILREPR = { -0x80, 0 };
    private final ByteBuffer encoded = ByteBuffer.allocate(16);
    private long itemCounter = 1; // valid values are > 0
    private final long[] lastShort = new long[129]; // pos 0 is used for empty, 128 for null
    private Cache currentCache;
    private Cache previousCache;

    /**
     * Construct a BackRefEncoder.
     */
    public BackRefEncoder(int cacheSize) {
        if (cacheSize > 0) {
            cacheSize = Math.max(cacheSize, MIN_CACHE_SIZE);
            currentCache = new Cache(cacheSize);
            previousCache = new Cache(cacheSize);
        }
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
        } else if (item.remaining() == 0) {
            return examineShort(0, itemNr);
        } else if (item.remaining() == 1 && item.get(item.position()) >= 0) {
            return examineShort(item.get(item.position()), itemNr);
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
        if (item.remaining() > currentCache.maxItemSize())
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

    public void reset() {
        itemCounter = 1;
        Arrays.fill(lastShort, 0);
        if (currentCache != null) {
            currentCache.reset();
            previousCache.reset();
        }
    }

    private class Cache {
        private final int[] offsets;
        private final long[] itemNumbers;
        private final ByteBuffer text;

        private Cache(int size) {
            // We want to use up to 10% of the size for hash slots. each slot is 4 bytes
            int maxSlotPercentage = 10;
            int maxSlots = size * maxSlotPercentage / (100 * 4);
            int nslots = Integer.highestOneBit(maxSlots);
            size = Math.max(size - 4 * nslots, size - size * maxSlotPercentage / 100);

            offsets = new int[nslots];
            itemNumbers = new long[nslots];
            text = ByteBuffer.allocate(size);
            text.position(1); // 0 reserved for 'not present'
        }

        void reset() {
            // 0 is reserved for 'not present'
            Arrays.fill(offsets, 0);
            text.position(1);
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
            return (int)(neverNegative % offsets.length);
        }

        public int maxItemSize() {
            return text.limit() / 3;
        }
    }
}
