package org.monetdb.spark.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.monetdb.spark.util.BackRefVerifierStream.State.*;

public class BackRefVerifierStream extends OutputStream {
    private final ByteBuffer[] expectedItems;
    private State state;
    long bytesSeen;            // bytes written so far
    long bytesAccepted;        // number of incoming bytes verified as correct
    int itemsAccepted;        // number of items accepted as correct
    private long accumulator;  // used to collect long backref bits in
    private int shift;        // bit position to insert next backref bits at

    public BackRefVerifierStream(ByteBuffer[] expectedItems) {
        this.expectedItems = expectedItems;
        state = Starting;
        bytesSeen = bytesAccepted = 0;
        itemsAccepted = 0;
        accumulator = 0;
        shift = 0;
    }

    @Override
    public void write(int b) throws IOException {
        try {
            process(b);
            bytesSeen++;
        } catch (AssertionError e) {
            String message = "After %d+%d=%d bytes, %d items: %s".formatted(bytesAccepted, bytesSeen - bytesAccepted, bytesSeen, itemsAccepted, e.getMessage());
            throw new AssertionError(message, e);
        }
    }

    private void process(int b) {
        assertTrue((accumulator == 0 && shift == 0) || state == NullOrLongBackRef, "inconsistent internal state");
        assertTrue(itemsAccepted < expectedItems.length, "did not expect more output");
        switch (state) {
            case InString -> {
                ByteBuffer expected = expectedItems[itemsAccepted];
                assertNotNull(expected);
                int pos = (int)(bytesSeen - bytesAccepted);
                if (pos == expected.remaining()) {
                    assertEquals(0, b);
                    moveOn();
                } else {
                    assertEquals(expected.get(expected.position() + pos), (byte)b);
                }
            }
            case NullOrLongBackRef -> {
                boolean last = b >= 0;
                long payload = (b + 256) & 0x7F;
                accumulator += payload << shift;
                shift += 7;
                assertTrue(accumulator >= 0);
                assertTrue(accumulator <= itemsAccepted, "backref=" + accumulator + ", only have " + itemsAccepted);
                if (last) {
                    if (accumulator == 0 && shift == 7) {
                        // It's not a backref, just the nil encoding 0x80 0x00
                        assertNull(expectedItems[itemsAccepted]);
                        moveOn();
                    } else {
                        checkBackrefAndMoveOn((int)accumulator);
                    }
                }
            }
            case Starting -> {
                if (b >= 0) {
                    // plain string
                    state = InString;
                    process(b);
                } else if (b == -0x80) {
                    state = NullOrLongBackRef;
                } else {
                    int delta = b - (-0x80);
                    assertTrue(delta > 0 && delta < 64, "invalid short delta " + delta);
                    checkBackrefAndMoveOn(delta);
                }
            }
        }
    }

    private void checkBackrefAndMoveOn(int n) {
        assertTrue(n > 0);
        assertTrue(n <= itemsAccepted);  // hence itemsAccepted-n will be >= 0
        ByteBuffer expected = expectedItems[itemsAccepted];
        ByteBuffer referenced = expectedItems[itemsAccepted - n];
        assertEquals(expected, referenced);
        moveOn();
    }

    private void moveOn() {
        bytesAccepted = bytesSeen + 1; // current byte must be included but bytesSeen hasn't been incremented yet
        itemsAccepted++;
        state = Starting;
        accumulator = 0;
        shift = 0;
    }

    @Override
    public void close() throws IOException {
        assertEquals(Starting, state);
        assertEquals(expectedItems.length, itemsAccepted);
        assertEquals(bytesSeen, bytesAccepted);
    }

    static enum State {
        Starting,
        InString,
        NullOrLongBackRef,
    }
}
