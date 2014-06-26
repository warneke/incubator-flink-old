package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicReference;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class BlobService {

	static final int TRANSFER_BUFFER_SIZE = 4096;

	static final byte PUT_OPERATION = 0;

	static final byte GET_OPERATION = 1;

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	private static final AtomicReference<AbstractBaseImpl> BLOB_SERVICE_IMPL = new AtomicReference<AbstractBaseImpl>(
		null);

	private BlobService() {
	}

	public static void initProxy(final InetSocketAddress serverAddress) throws IOException {

		while (true) {

			if (BLOB_SERVICE_IMPL.get() != null) {
				return;
			}

			final ProxyImpl proxyImpl = new ProxyImpl(serverAddress);
			if (BLOB_SERVICE_IMPL.compareAndSet(null, proxyImpl)) {
				return;
			}
		}
	}

	public static void initServer(final InetSocketAddress socketAddress) throws IOException {

		while (true) {

			if (BLOB_SERVICE_IMPL.get() != null) {
				return;
			}

			final ServerImpl serverImpl = new ServerImpl(socketAddress);
			if (BLOB_SERVICE_IMPL.compareAndSet(null, serverImpl)) {
				serverImpl.start();
				return;
			}
		}
	}

	private static AbstractBaseImpl get() {

		final AbstractBaseImpl impl = BLOB_SERVICE_IMPL.get();
		if (impl == null) {
			throw new IllegalStateException("BLOB service has not been initalized yet");
		}

		return impl;
	}

	public static BlobKey put(final JobID jobID, final byte[] buf) throws IOException {

		return get().put(jobID, buf, 0, buf.length);
	}

	public static BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		return get().put(jobID, buf, offset, len);
	}

	public static BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {

		return get().put(jobID, inputStream);
	}

	public static BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len,
			final InetSocketAddress serverAddr) throws IOException {

		final byte[] lenBuf = new byte[4];

		Socket socket = null;
		try {
			socket = new Socket(serverAddr.getAddress(), serverAddr.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(BlobService.PUT_OPERATION);
			sendJobID(jobID, os);
			final MessageDigest md = getMessageDigest();

			int bytesSent = 0;
			while (bytesSent < len) {

				final int bytesToSend = Math.min(BlobService.TRANSFER_BUFFER_SIZE, len - bytesSent);
				writeLength(bytesToSend, lenBuf, os);
				md.update(buf, offset + bytesSent, bytesToSend);
				os.write(buf, offset + bytesSent, bytesToSend);
				bytesSent += bytesToSend;
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

	public static BlobKey put(final JobID jobID, final InputStream inputStream, final InetSocketAddress serverAddr)
			throws IOException {

		final byte[] lenBuf = new byte[4];
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];

		Socket socket = null;
		try {
			socket = new Socket(serverAddr.getAddress(), serverAddr.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(BlobService.PUT_OPERATION);
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
	 * Auxiliary method to finish a PUT operation over the network.
	 * 
	 * @param inputStream
	 *        the input stream to read the verification BLOB key from.
	 * @return the server's BLOB key of the uploaded BLOB for verification
	 * @throws IOException
	 *         thrown if an I/O error occurs during the data transfer
	 */
	private static final BlobKey finishPut(final InputStream inputStream) throws IOException {

		final BlobKey key = BlobKey.readFromInputStream(inputStream);

		// Next byte must be end of stream
		if (inputStream.read() >= 0) {
			throw new IOException("Received unexpected input while trying to finish put operation");
		}

		return key;
	}

	/**
	 * Auxiliary method to silently close a network socket.
	 * 
	 * @param socket
	 *        the network socket to close, possibly <code>null</code>
	 */
	static void closeSilently(final Socket socket) {

		if (socket != null) {
			try {
				socket.close();
			} catch (IOException ioe) {
			}
		}
	}

	/**
	 * Auxiliary method to write the length of an upcoming data chunk to an output stream.
	 * 
	 * @param length
	 *        the length of the upcoming data chunk in bytes
	 * @param buf
	 *        the byte buffer to use for the integer serialization
	 * @param outputStream
	 *        the output stream to write the length to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the output stream
	 */
	private static void writeLength(final int length, final byte[] buf, final OutputStream outputStream)
			throws IOException {

		buf[0] = (byte) (length & 0xff);
		buf[1] = (byte) ((length >> 8) & 0xff);
		buf[2] = (byte) ((length >> 16) & 0xff);
		buf[3] = (byte) ((length >> 24) & 0xff);

		outputStream.write(buf);
	}

	/**
	 * Auxiliary method to read the length of an upcoming data chunk from an input stream.
	 * 
	 * @param buf
	 *        the byte buffer to use for the integer deserialization
	 * @param inputStream
	 *        the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading from the input stream
	 */
	static int readLength(final byte[] buf, final InputStream inputStream) throws IOException {

		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		bytesRead = buf[0] & 0xff;
		bytesRead |= (buf[1] & 0xff) << 8;
		bytesRead |= (buf[2] & 0xff) << 16;
		bytesRead |= (buf[3] & 0xff) << 24;

		return bytesRead;
	}

	/**
	 * Auxiliary method to write a {@link JobID} to an output stream.
	 * 
	 * @param jobID
	 *        the job ID to write, possibly <code>null</code>
	 * @param outputStream
	 *        the output stream to write the ID to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the ID
	 */
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

	/**
	 * Auxiliary method to read a {@link JobID} from an input stream.
	 * 
	 * @param inputStream
	 *        the input stream to read the job ID from
	 * @return the read job ID, possibly <code>null</code>
	 * @throws IOException
	 *         throw if an I/O error occurs while reading the ID
	 */
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

	/**
	 * Opens an input stream to the BLOB with the given key. Calling this method requires previous initialization of the
	 * BLOB service.
	 * 
	 * @param key
	 *        the key of the BLOB to retrieve
	 * @return an input stream to the BLOB with the given key
	 * @throws FileNotFoundException
	 *         thrown if the BLOB with the given key could not be found
	 * @throws IOException
	 *         thrown if an error occurs during the BLOB transfer
	 */
	public static InputStream get(final BlobKey key) throws IOException {

		return get().get(key);
	}

	public static InputStream get(final BlobKey key, final InetSocketAddress serverAddr) throws IOException {

		Socket socket = null;
		int status = 0;
		try {
			socket = new Socket(serverAddr.getAddress(), serverAddr.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(BlobService.GET_OPERATION);
			key.writeToOutputStream(os);
			os.flush();

			final InputStream is = socket.getInputStream();
			status = is.read();
			if (status < 0) {
				throw new EOFException();
			} else if (status == 0) {
				throw new FileNotFoundException();
			}

			return is;

		} finally {
			if (status <= 0) {
				closeSilently(socket);
			}
		}
	}

	/**
	 * Returns the URL of the BLOB with the given key. Calling this method requires previous initialization of the
	 * BLOB service.
	 * 
	 * @param key
	 *        the key of the BLOB to retrieve
	 * @return the URL of the BLOB with the given key
	 * @throws FileNotFoundException
	 *         thrown if the BLOB with the given key could not be found
	 * @throws IOException
	 *         thrown if an error occurs during the BLOB transfer
	 */
	public static URL getURL(final BlobKey key) throws IOException {

		return get().getURL(key);
	}

	/**
	 * Shuts down the BLOB service, closes all open network ports and deletes the BLOB storage or cache, respectively.
	 * Calling this method requires previous initialization of the BLOB service.
	 */
	public static void shutdown() {

		get().shutdown();
	}
}
