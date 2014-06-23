package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;

import eu.stratosphere.nephele.jobgraph.JobID;

final class ProxyImpl extends AbstractBaseImpl {

	private final InetSocketAddress serverAddress;

	ProxyImpl(final InetSocketAddress serverAddress) throws IOException {
		super();

		this.serverAddress = serverAddress;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		final byte[] lenBuf = new byte[4];

		Socket socket = null;
		try {
			socket = new Socket(this.serverAddress.getAddress(), this.serverAddress.getPort());
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {

		final byte[] lenBuf = new byte[4];
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];

		Socket socket = null;
		try {
			socket = new Socket(this.serverAddress.getAddress(), this.serverAddress.getPort());
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
	 * {@inheritDoc}
	 */
	@Override
	InputStream get(final BlobKey key) throws IOException {

		final File blob = getLocal(key);
		if (blob != null) {
			return new FileInputStream(blob);
		}

		return getFromServer(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void shutdown() {
		// TODO Wipe the storage directory
	}

	private InputStream getFromServer(final BlobKey key) throws IOException {

		Socket socket = null;
		int status = 0;
		try {
			socket = new Socket(this.serverAddress.getAddress(), this.serverAddress.getPort());
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

	private static final BlobKey finishPut(final InputStream inputStream) throws IOException {

		final BlobKey key = BlobKey.readFromInputStream(inputStream);

		// Next byte must be end of stream
		if (inputStream.read() >= 0) {
			throw new IOException("Received unexpected input while trying to finish put operation");
		}

		return key;
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
