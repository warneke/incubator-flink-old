package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.util.StringUtils;

public final class BlobManager extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobManager.class);

	/**
	 * The server socket the BLOB manager listens on for incoming connections.
	 */
	private final ServerSocket serverSocket;

	/**
	 * Indicates whether a shutdown of the BLOB manager has been requested.
	 */
	private volatile boolean shutdownRequested = false;

	public BlobManager(final InetAddress address, final int port) throws IOException {
		this(new InetSocketAddress(address, port));
	}

	public BlobManager(final InetSocketAddress socketAddress) throws IOException {
		super("BLOB manager");

		this.serverSocket = new ServerSocket();
		this.serverSocket.bind(socketAddress);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info("Started BLOB manager on "
				+ this.serverSocket.getLocalSocketAddress());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!this.shutdownRequested) {

			try {

				final Socket socket = this.serverSocket.accept();
				new BlobConnection(socket).start();

			} catch (IOException ioe) {
				if (!this.shutdownRequested && LOG.isErrorEnabled()) {
					LOG.error(StringUtils.stringifyException(ioe));
				}
			}
		}
	}

	/**
	 * Shuts down the BLOB manager.
	 */
	public void shutdown() {

		this.shutdownRequested = true;
		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ioe));
			}
		}

		try {
			join();
		} catch (InterruptedException ie) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ie));
			}
		}
	}

}
