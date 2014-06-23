package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.util.StringUtils;

final class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	private final ServerImpl blobServer;

	private final Socket socket;

	BlobConnection(final ServerImpl blobServer, final Socket socket) {
		super("BLOB connection from " + socket.getRemoteSocketAddress());

		this.blobServer = blobServer;
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
				get(is, this.socket.getOutputStream());
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

	private void put(final InputStream inputStream, final OutputStream outputStream) throws IOException {

		final BlobKey key = this.blobServer.putFromNetwork(inputStream);

		// Send the key back to the client for verification
		key.writeToOutputStream(outputStream);
	}

	private void get(final InputStream inputStream, final OutputStream outputStream) throws IOException {

		// Receive the blob key
		final BlobKey key = BlobKey.readFromInputStream(inputStream);

		final InputStream fis = this.blobServer.get(key);
		if (fis == null) {
			outputStream.write(0);
			return;
		}

		outputStream.write(1);
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];

		try {

			while (true) {

				final int read = fis.read(buf);
				if (read < 0) {
					break;
				}

				outputStream.write(buf, 0, read);
			}

		} finally {
			fis.close();
		}
	}
}
