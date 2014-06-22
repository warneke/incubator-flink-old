package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class BlobService {

	/**
	 * Algorithm to be used for calculating the library signatures.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	static final int TRANSFER_BUFFER_SIZE = 4096;

	static final byte PUT_OPERATION = 0;

	static final byte GET_OPERATION = 1;

	private BlobService() {
	}

	public static BlobKey put(final JobID jobID, final byte[] buf, final InetSocketAddress addr) throws IOException {

		return put(jobID, buf, 0, buf.length, addr);
	}

	public static BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len,
			final InetSocketAddress addr) throws IOException {

		final byte[] lenBuf = new byte[4];

		Socket socket = null;
		try {
			socket = new Socket(addr.getAddress(), addr.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(PUT_OPERATION);
			sendJobID(jobID, os);

			final MessageDigest md = getMessageDigest();
			md.update(buf, offset, len);
			writeLength(len, lenBuf, os);
			os.write(buf, offset, len);
			writeLength(-1, lenBuf, os);
			os.flush();

			final BlobKey localKey = new BlobKey(md.digest());
			final BlobKey remoteKey = finishPut(socket.getInputStream());

			if (!localKey.equals(remoteKey)) {
				throw new IOException("Detected data corruption during transfer");
			}

			return localKey;

		} finally {
			closeSilently(socket);
		}
	}

	public static BlobKey put(final JobID jobID, final InputStream inputStream, final InetSocketAddress addr)
			throws IOException {

		final byte[] lenBuf = new byte[4];
		final byte[] buf = new byte[TRANSFER_BUFFER_SIZE];

		Socket socket = null;
		try {
			socket = new Socket(addr.getAddress(), addr.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(PUT_OPERATION);
			sendJobID(jobID, os);
			final MessageDigest md = getMessageDigest();

			while (true) {

				final int read = inputStream.read(buf);
				if (read < 0) {
					break;
				}

				md.update(buf, 0, read);
				writeLength(read, lenBuf, os);
				os.write(buf, 0, read);
			}

			writeLength(-1, lenBuf, os);
			os.flush();

			final BlobKey localKey = new BlobKey(md.digest());
			final BlobKey remoteKey = finishPut(socket.getInputStream());

			if (!localKey.equals(remoteKey)) {
				throw new IOException("Detected data corruption during transfer");
			}

			return localKey;

		} finally {
			closeSilently(socket);
		}
	}

	/**
	 * Returns an instance of the message digest to use for the BLOB key computation.
	 * 
	 * @return an instance of the message digest to use for the BLOB key computation
	 */
	static MessageDigest getMessageDigest() {

		try {
			return MessageDigest.getInstance(HASHING_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	static void sendJobID(final JobID jobID, final OutputStream outputStream) throws IOException {

		if (jobID == null) {
			// Write 0 to indicate no job ID is following
			outputStream.write(0);
			return;
		}

		// Write 1 to indicate a job ID is following
		outputStream.write(1);
		final byte[] buf = new byte[JobID.SIZE];
		final ByteBuffer bb = ByteBuffer.wrap(buf);
		jobID.write(bb);
		outputStream.write(buf);
	}

	static JobID receiveJobID(final InputStream inputStream) throws IOException {

		int read = inputStream.read();
		if (read < 0) {
			throw new EOFException();
		} else if (read == 0) {
			return null;
		}

		final byte[] buf = new byte[JobID.SIZE];
		int bytesRead = 0;
		while (bytesRead < JobID.SIZE) {
			read = inputStream.read(buf, bytesRead, JobID.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		return new JobID(buf);
	}

	private static final BlobKey finishPut(final InputStream inputStream) throws IOException {

		final byte[] key = new byte[BlobKey.SIZE];

		int bytesRead = 0;
		while (bytesRead < BlobKey.SIZE) {
			final int read = inputStream.read(key, bytesRead, BlobKey.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		// Next byte must be end of stream
		if (inputStream.read() >= 0) {
			throw new IOException("Received unexpected input while trying to finish put operation");
		}

		return new BlobKey(key);
	}

	static void writeLength(final int length, final byte[] buf, final OutputStream outputStream) throws IOException {

		buf[0] = (byte) (length & 0xff);
		buf[1] = (byte) ((length >> 8) & 0xff);
		buf[2] = (byte) ((length >> 16) & 0xff);
		buf[3] = (byte) ((length >> 24) & 0xff);

		outputStream.write(buf);
	}

	static int readLength(final byte[] buf, final InputStream inputStream) throws IOException {

		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		bytesRead = buf[0];
		bytesRead |= (buf[1] & 0xff) << 8;
		bytesRead |= (buf[2] & 0xff) << 16;
		bytesRead |= (buf[3] & 0xff) << 24;

		return bytesRead;
	}

	private static void closeSilently(final Socket socket) {

		if (socket != null) {
			try {
				socket.close();
			} catch (IOException ioe) {
			}
		}
	}
}
