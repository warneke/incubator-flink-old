package eu.stratosphere.nephele.services.blob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import eu.stratosphere.core.io.IOReadableWritable;

public final class BlobKey implements IOReadableWritable, Comparable<BlobKey> {

	private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

	private static final int SIZE = 20;

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

	static BlobKey readFromInputStream(final InputStream inputStream) throws IOException {

		final byte[] key = new byte[BlobKey.SIZE];

		int bytesRead = 0;
		while (bytesRead < BlobKey.SIZE) {
			final int read = inputStream.read(key, bytesRead, BlobKey.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		return new BlobKey(key);
	}

	void writeToOutputStream(final OutputStream outputStream) throws IOException {

		outputStream.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final BlobKey o) {

		final byte[] aarr = this.key;
		final byte[] barr = o.key;
		final int len = Math.min(aarr.length, barr.length);

		for (int i = 0; i < len; ++i) {
			final int a = (aarr[i] & 0xff);
			final int b = (barr[i] & 0xff);
			if (a != b) {
				return a - b;
			}
		}

		return aarr.length - barr.length;
	}

	public void addToMessageDigest(final MessageDigest md) {

		md.update(this.key);
	}
}
