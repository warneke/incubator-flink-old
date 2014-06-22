package eu.stratosphere.nephele.services.blob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.core.io.IOReadableWritable;

public final class BlobKey implements IOReadableWritable {

	private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

	static final int SIZE = 20;

	private final byte[] key;

	public BlobKey() {
		this.key = new byte[SIZE];
	}

	BlobKey(final byte[] key) {

		if (key.length != SIZE) {
			throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
		}

		this.key = key;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		in.readFully(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof BlobKey)) {
			return false;
		}

		final BlobKey bk = (BlobKey) obj;

		return Arrays.equals(this.key, bk.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final char[] hexChars = new char[SIZE * 2];
		for (int i = 0; i < SIZE; ++i) {
			int v = this.key[i] & 0xff;
			hexChars[i * 2] = HEX_ARRAY[v >>> 4];
			hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0f];
		}

		return new String(hexChars);
	}
}
