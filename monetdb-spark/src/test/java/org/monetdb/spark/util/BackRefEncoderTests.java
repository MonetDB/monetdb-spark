package org.monetdb.spark.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
