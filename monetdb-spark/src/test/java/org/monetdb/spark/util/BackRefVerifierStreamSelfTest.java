package org.monetdb.spark.util;


import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The Verifier is a nontrivial amount of code, it needs tests of its own
 */
class BackRefVerifierStreamSelfTest {
    int overrideIndex = -1;
    ByteBuffer overrideItem = null;

    ByteBuffer[] makeBuffers(String... strings) {
        ByteBuffer[] bufs = new ByteBuffer[strings.length];
        for (int i = 0; i < bufs.length; i++) {
            String s = strings[i];
            ByteBuffer bb = s == null ? null : ByteBuffer.wrap(s.getBytes(UTF_8));
            bufs[i] = bb;
        }
        return bufs;
    }

    void feedVerifier(String... strings) throws IOException {
        ByteBuffer[] items = makeBuffers(strings);
        OutputStream vs = new BackRefVerifierStream(items);
        BackRefEncoder enc = new BackRefEncoder( 1 << 16);
        for (int i = 0; i < items.length; i++) {
            ByteBuffer item = (i == overrideIndex) ? overrideItem : items[i];
            enc.write(item, vs);
        }
        vs.close();
    }

    private void setupOverride(int i, String b) {
        overrideIndex = i;
        overrideItem = b == null ? null : ByteBuffer.wrap(b.getBytes(UTF_8));
    }

    @Test
    public void testImmediateClose() throws IOException {
        feedVerifier();
    }

    @Test
    public void testNoRepeatSingle() throws IOException {
        feedVerifier("A", "B", null, "", "C");
    }

    @Test
    public void testWrongItem() throws IOException {
        setupOverride(1, "b");
        AssertionError exc = assertThrows(
                AssertionError.class,
                () -> feedVerifier("A", "B", null, "", "C")
        );
        assertTrue(exc.getMessage().contains("2+0=2"), exc.getMessage());
    }

    @Test
    public void testUnexpectedNull() throws IOException {
        setupOverride(1, null);
        AssertionError exc = assertThrows(
                AssertionError.class,
                () -> feedVerifier("A", "B", null, "", "C")
        );
        assertTrue(exc.getMessage().contains("2+1=3"), exc.getMessage());
    }

    @Test
    public void testUnexpectedNonnull() throws IOException {
        setupOverride(2, "x");
        AssertionError exc = assertThrows(
                AssertionError.class,
                () -> feedVerifier("A", "B", null, "", "C")
        );
        assertTrue(exc.getMessage().contains("4+0=4"), exc.getMessage());
    }

    @Test
    public void testRepeatSingle() throws IOException {
        feedVerifier("A", "A", "", "", null, null, "A");
    }

    @Test
    public void testNoRepeatMulti() throws IOException {
        feedVerifier("aap", "noot", null, "", "mies");
    }

    @Test
    public void testFarBack() throws IOException {
        int n = 3000;
        String[] inputs = new String[n];

        // Set up basic non-repeating data
        for (int i = 0; i < n; i++) {
            inputs[i] = "s" + i;
        }

        // Introduce some repetitions:
        inputs[100] = inputs[100 - 30];
        inputs[200] = inputs[200 - 63];

        // Long
        inputs[300] = inputs[300 - 64];
        inputs[1000] = inputs[1000 - 500];

        feedVerifier(inputs);
    }

}