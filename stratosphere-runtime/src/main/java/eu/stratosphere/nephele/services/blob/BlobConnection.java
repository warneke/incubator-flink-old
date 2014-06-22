package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

final class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	private final Socket socket;

	BlobConnection(final Socket socket) {
		super("BLOB connection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		try {
			final InputStream is = this.socket.getInputStream();
			int operation = is.read();
			if (operation < 0) {
				return;
			}

			switch (operation) {
			case BlobService.PUT_OPERATION:
				put(is, this.socket.getOutputStream());
				break;
			case BlobService.GET_OPERATION:
				get();
				break;
			default:
				if (LOG.isErrorEnabled()) {
					LOG.error("Received unknown operation code " + operation);
				}
			}

		} catch (IOException ioe) {
			if (LOG.isErrorEnabled()) {
				LOG.error(StringUtils.stringifyException(ioe));
			}
		} finally {
			try {
				this.socket.close();
			} catch (IOException ioe) {
			}
		}
	}

	private static void put(final InputStream inputStream, final OutputStream outputStream) throws IOException {

		final JobID jobID = BlobService.receiveJobID(inputStream);

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];
		final byte[] lenBuf = new byte[4];

		while (true) {

			final int bytesToReceive = BlobService.readLength(lenBuf, inputStream);
			if (bytesToReceive < 0) {
				break;
			}

			int bytesInBuffer = 0;
			while (bytesInBuffer < bytesToReceive) {
				final int read = inputStream.read(buf, bytesInBuffer, bytesToReceive - bytesInBuffer);
				if (read < 0) {
					throw new EOFException();
				}
				bytesInBuffer += read;
			}

			md.update(buf, 0, bytesInBuffer);
			// TODO: Write the data somewhere
		}

		final byte[] digest = md.digest();
		final BlobKey key = new BlobKey(digest);

		// Send the key back to the client for verification
		outputStream.write(digest);

		// TODO: Register BLOB with BLOB manager

	}

	private static void get() throws IOException {
		// TODO: Implement me
	}
}
