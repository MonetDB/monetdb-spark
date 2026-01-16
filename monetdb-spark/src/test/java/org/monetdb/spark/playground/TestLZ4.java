package org.monetdb.spark.playground;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import net.jpountz.lz4.LZ4FrameOutputStream.FLG;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TestLZ4 {

	private final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
	private final XXHashFactory hashFactory = XXHashFactory.fastestInstance();

	@Test
	public void testRoundtrip() throws IOException {
		int n = 1024;
		byte[] plainData = new byte[1024];
		for (int i = 0; i < n; i++) {
			plainData[i] = (byte)i;
		}

		var dest = new ByteArrayOutputStream();
		var compress = new LZ4FrameOutputStream(dest);
		compress.write(plainData);
		compress.close();
		byte[] compressedData = dest.toByteArray();

		var src = new ByteArrayInputStream(compressedData);
		var decompress = new LZ4FrameInputStream(src);
		var buffer = new byte[5 * n];
		int nread = decompress.read(buffer);
		assertEquals(n, nread);
		var decompressedData = Arrays.copyOfRange(buffer, 0, nread);
		assertArrayEquals(plainData, decompressedData);
		decompress.close();
	}

	@Test
	public void testInterop() throws IOException {
		int n = 1024;
		var compressed = new byte[] {
				0x4, 0x22, 0x4d, 0x18, 0x64, 0x40, -0x59, 0xe,
				0x0, 0x0, 0x0, 0x1f, 0x0, 0x1, 0x0, -0x1, -0x1,
				-0x1, -0x16, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, -0x18, -0x34, -0x5a, -0x2e,
		};
		var expected = new byte[n];

		var decompress = new LZ4FrameInputStream(new ByteArrayInputStream(compressed));
		var buffer = new byte[5 * n];
		int nread = decompress.read(buffer);
		assertEquals(n, nread);
		var decompressedData = Arrays.copyOfRange(buffer, 0, nread);
		assertArrayEquals(expected, decompressedData);
		decompress.close();
	}

	@Test
	public void testCompressionLevel() throws IOException {
		InputStream stream = TestLZ4.class.getResourceAsStream("/log4j2.properties");
		assertNotNull(stream);
		byte[] testData = IOUtils.toByteArray(stream);
		int level2Size = compressedSize(testData, 2);
		int level17Size = compressedSize(testData, 17);
		assertTrue(level17Size < level2Size, String.format("Expected level17size=%d to be less than level2size=%d", level17Size, level2Size));
	}

	private int compressedSize(byte[] testData, int level) throws IOException {
		LZ4Compressor compressor = lz4Factory.highCompressor(level);
		var dest = new ByteArrayOutputStream();
		XXHash32 hasher = hashFactory.hash32();
		var compress = new LZ4FrameOutputStream(dest, BLOCKSIZE.SIZE_4MB, 0, compressor, hasher, FLG.Bits.BLOCK_INDEPENDENCE);
		compress.write(testData);
		compress.close();
		return dest.size();
	}
}
