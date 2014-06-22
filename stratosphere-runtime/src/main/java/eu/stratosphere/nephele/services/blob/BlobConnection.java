package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.util.StringUtils;

final class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	private static final int PUT_OPERATION = 0;

	private static final int GET_OPERATION = 1;

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
			case PUT_OPERATION:
				put();
				break;
			case GET_OPERATION:
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

	private static void put() throws IOException {
		//TODO: Implement me
	}

	private static void get() throws IOException {
		//TODO: Implement me
	}
}
