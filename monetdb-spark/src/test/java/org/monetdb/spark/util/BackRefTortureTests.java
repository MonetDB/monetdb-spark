package org.monetdb.spark.util;

import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class BackRefTortureTests {
    // For reproducibility we use our own simple RNG here.
    // It's xorshift32, translated to Java from the C code on Wikipedia.
    // We use long because Java has no unsigned types.
    // The value will always be 0 <= n < (1<<32)
    long xorshift32_state;

    @Test
    public void testInput1() throws IOException {
        randomizedTest(1);
    }

    @Test
    public void testInput2() throws IOException {
        randomizedTest(2);
    }

    @Test
    public void testManyInputs() {
        var rng = new Random();
        long t0 = System.currentTimeMillis();
        long deadline = t0 + Config.tortureDurationMillis();
        long count = 0;
        while (System.currentTimeMillis() <= deadline) {
            long n = rng.nextLong();
            if (n <= 0)
                continue;
            testOneInput(n);
            count++;
        }
        long duration = System.currentTimeMillis() - t0;
        double rate = 1000.0 * count / duration;
        System.err.printf("Tried %d inputs in %s millis: %.1f input/s%n", count, duration, rate);

    }

    private void testOneInput(long n) {
        try {
            // if this fails, we must add a standalone test for this n
            randomizedTest(n);
        } catch (Throwable e) {
            throw new AssertionError("Input " + n + " failed: " + e.getMessage(), e);
        }
    }

    private void randomizedTest(long n) throws IOException {
        assertNotEquals(0, n);
        xorshift32_state = n;

        int nitems = 100_000;
        ByteBuffer[] inputs = new ByteBuffer[nitems];

        // Of the strings we use, a certain fraction comes from
        // a repeating vocabulary, a certain fraction is unique and a certain
        // fraction is null

        // Pick the parameters
        int nvoc;
        int nvocnul;
        int nvocnuluniq;
        ByteBuffer[] vocabulary;
        do {
            int rand = nextRand(10);
            nvoc = switch (rand) {
                case 0 -> 0;
                case 1 -> nextRand(5);
                case 2 -> nextRand(100);
                default -> nextRand(nitems);
            };
            vocabulary = new ByteBuffer[nvoc];
            for (int i = 0; i < nvoc; i++)
                vocabulary[i] = nextStr();

            int nullFraction = switch (nextRand(10)) {
                case 0 -> 0;
                case 1 -> 0;
                case 2 -> 0;
                case 3 -> nextRand(nitems);
                default -> nextRand(nitems / 10);
            };
            nvocnul = nvoc + nullFraction;

            int uniqueFraction = nextRand(10) < 1 ? 0 : nextRand(nitems);
            nvocnuluniq = nvocnul + uniqueFraction;
        } while (nvocnuluniq == 0);

        // Generate the inputs
        for (int i = 0; i < nitems; i++) {
            n = nextRand(nvocnuluniq);
            if (n < nvoc)
                inputs[i] = vocabulary[nextRand(vocabulary.length)];
            else if (n < nvocnul)
                inputs[i] = null;
            else
                inputs[i] = nextStr();
        }

        // Run the test
        BackRefVerifierStream verifier = new BackRefVerifierStream(inputs);
        BackRefEncoder enc = new BackRefEncoder();
        for (ByteBuffer bb : inputs) {
            enc.write(bb, verifier);
        }
        verifier.close();

    }

    long nextRand() {
        long x = xorshift32_state;
        // regular algorithm
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        // simulate unsigned int
        if (x < 0)
            x += (1L<<32);
        x &= 0xFFFF_FFFFL;
        // wrap up
        xorshift32_state = x;
        return x;
    }

    int nextRand(int n) {
        // we don't care about the bias this introduces
        return (int) (nextRand() % n);
    }

    private ByteBuffer nextStr() {
        int lengthClass = nextRand(5);
        int n = switch (lengthClass) {
            case 0 -> 0;
            case 1 -> 1;
            case 3 -> nextRand(25) == 0 ? nextRand(100_000) : nextRand(100);
            default -> nextRand(10);
        };
        ByteBuffer bb = ByteBuffer.allocate(n);
        for (int i = 0; i < n; i++)
            bb.put((byte)(nextRand(95) + 32));
        bb.flip();
        return bb;
    }

}
