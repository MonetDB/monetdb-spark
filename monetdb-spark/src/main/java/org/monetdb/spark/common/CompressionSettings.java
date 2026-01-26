package org.monetdb.spark.common;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A helper class that can parse strings like "lz4:5" and can wrap
 * Outputstreams in the requested compression algorithm.
 */
public class CompressionSettings implements Serializable {
	private final String algo;
	private final int level;
	private static LZ4Factory lz4factory;
	private static XXHashFactory hashfactory;

	public CompressionSettings() {
		this("none");
	}

	public CompressionSettings(String desc) {
		String[] parts = desc.split(":", 2);
		if (parts[0].equals("none"))
			algo = null;
		else if (parts[0].equals("lz4"))
			algo = "lz4";
		else
			throw new IllegalArgumentException("Unsupported compression algorithm: " + parts[0]);

		if (parts.length == 1) {
			level = 0;
			return;
		}

		int n;
		try {
			n = Integer.parseInt(parts[1]);
		} catch (NumberFormatException e) {
			n = -1;
		}
		if (n < 0 || n > 17)
			throw new IllegalArgumentException("Invalid compression level: " + parts[1]);

		level = n;
	}

	public String algo() {
		return algo;
	}

	public OutputStream wrap(OutputStream inner) {
		if (algo == null)
			return inner;
		else if (algo.equals("lz4"))
			return wrapLz4(inner);
		else
			throw new IllegalArgumentException("Not implemented yet: " + algo);
	}

	public OutputStream wrapLz4(OutputStream inner) {
		synchronized (CompressionSettings.class) {
			if (lz4factory == null) {
				lz4factory = LZ4Factory.fastestInstance();
			}
			if (hashfactory == null) {
				hashfactory = XXHashFactory.fastestInstance();
			}
		}

		LZ4Compressor compressor = level != 0
				? lz4factory.highCompressor()
				: lz4factory.fastCompressor();
		XXHash32 hasher = hashfactory.hash32();
		LZ4FrameOutputStream compressed;
		try {
			compressed = new LZ4FrameOutputStream(inner, LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB, 0, compressor, hasher, LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
		} catch (IOException e) {
			throw new RuntimeException("Could not wrap compression", e);
		}
		return compressed;
	}
}
