package org.monetdb.spark.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.monetdb.spark.util.Assertions.dollarEscape;

class BackRefEncoderTests {
    private byte[] encode(String[] inputs) throws IOException {
        BackRefEncoder be = new BackRefEncoder();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        for (String input: inputs) {
            ByteBuffer buf = input != null ? ByteBuffer.wrap(input.getBytes(UTF_8)) : null;
            be.write(buf, sink);
        }
        return sink.toByteArray();
    }

    private String qencode(String... inputs) throws IOException {
        return dollarEscape(encode(inputs));
    }

    @Test
    public void testNoRepeatSingle() throws IOException {
        assertEquals("A$00" + "B$00" + "$80$00" + "$00" +"C$00", qencode("A", "B", null, "", "C"));
    }

    @Test
    public void testNullNull() throws IOException {
        assertEquals("A$00$80$00$81", qencode("A", null, null));
    }

    @Test
    public void testRepeatSingle() throws IOException {
        assertEquals("A$00" + "$81" + "$00" + "$81" + "$80$00" + "$81" + "$85", qencode("A", "A", "", "", null, null, "A"));
    }

    @Test
    public void testNoRepeatMulti() throws IOException {
        assertEquals("aap$00" + "noot$00" + "$80$00" + "$00" +"mies$00", qencode("aap", "noot", null, "", "mies"));
    }

    @Test
    public void testFarBack() throws IOException {
        int n = 3000;
        String[] inputs = new String[n];
        String[] expected = new String[n];

        // Set up basic non-repeating data
        for (int i = 0; i < n; i++) {
            String s = "s" + i;
            inputs[i] = s;
            expected[i] = s + "$00";
        }

        // Introduce some repetitions:

        // Short
        inputs[100] = inputs[100 - 30];
        expected[100] = checkDelta(new byte[] { -0x80 + 30 }, 30);
        inputs[200] = inputs[200 - 63];
        expected[200] = checkDelta(new byte[] { -0x80 + 63 }, 63);

        // Long
        inputs[300] = inputs[300 - 64];
        expected[300] = checkDelta(new byte[] { -0x80, 64 }, 64);
        inputs[1000] = inputs[1000 - 500];
        // 500 = 3 * 128 + 116.
        expected[1000] = checkDelta(new byte[] { -0x80, -0x80 + 116, 3 }, 500);

        StringBuilder builder = new StringBuilder();
        for (String e: expected) {
            builder.append(e);
        }
        String fullExpected = builder.toString();
        assertEquals(fullExpected, qencode(inputs));
    }

    private String checkDelta(byte[] bytes, long expected) {
        // Short or long?
        if (bytes.length == 1) {
            // Short
            byte b = bytes[0];
            byte lo = -0x80 + 1;
            byte hi = -0x80 + 63;
            assertTrue(lo <= b && b <= hi, "lo=" + lo + " b=" + b + " hi=" + hi);
            assertEquals(expected, (long)(b & 0x7F));
        } else {

            // Long.
            // The first byte is always 0x80
            assertEquals(-0x80, bytes[0]);

            // All but the last byte have their high bit set
            for (int i = 1; i < bytes.length - 1; i++) {
                assertTrue(bytes[i] < 0, "bytes[" + i + "]=" + bytes[i]);
            }
            assertTrue(bytes[bytes.length - 1] >= 0);

            // The encoded number is what we expect
            long acc = 0;
            int shift = 0;
            for (int i = 1; i < bytes.length; i++) {
                long payload = bytes[i] & 0x7F;
                acc += payload << shift;
                shift += 7;
            }
            assertEquals(expected, acc);
        }

        // For convenience, we return the dollar encoding
        return dollarEscape(bytes);
    }

}
