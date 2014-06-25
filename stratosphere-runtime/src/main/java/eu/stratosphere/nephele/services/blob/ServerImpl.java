package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

final class ServerImpl extends AbstractBaseImpl implements Runnable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ServerImpl.class);

	/**
	 * The server socket the BLOB manager listens on for incoming connections.
	 */
	private final ServerSocket serverSocket;

	private final Thread serverThread;

	/**
	 * Indicates whether a shutdown of the BLOB manager has been requested.
	 */
	private volatile boolean shutdownRequested = false; // TODO: Reconsider this design

	ServerImpl(final InetSocketAddress socketAddress) throws IOException {
		super();

		this.serverSocket = new ServerSocket();
		this.serverSocket.bind(socketAddress);

		this.serverThread = new Thread(this);
	}

	@Override
	BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	BlobKey putFromNetwork(final InputStream inputStream) throws IOException {

		receiveJobID(inputStream);

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];
		final byte[] lenBuf = new byte[4];

		final File tempFile = createTempFile();
		final FileOutputStream fos = new FileOutputStream(tempFile);
		boolean deleteTempFile = true;
		try {

			while (true) {

				final int bytesToReceive = readLength(lenBuf, inputStream);
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
				fos.write(buf, 0, bytesInBuffer);
			}

			// Close file stream and compute key
			fos.close();
			final BlobKey key = new BlobKey(md.digest());
			tempFile.renameTo(keyToFilename(key));
			deleteTempFile = false;

			return key;

		} finally {
			fos.close();
			if (deleteTempFile) {
				tempFile.delete();
			}
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

		return null;
	}

	/**
	 * Shuts down the BLOB server.
	 */
	@Override
	void shutdown() {

		this.shutdownRequested = true;
		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ioe));
			}
		}

		try {
			this.serverThread.join();
		} catch (InterruptedException ie) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ie));
			}
		}

		// Wipe the storage directory
		super.shutdown();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!this.shutdownRequested) {

			try {

				final Socket socket = this.serverSocket.accept();
				new BlobConnection(this, socket).start();

			} catch (IOException ioe) {
				if (!this.shutdownRequested && LOG.isErrorEnabled()) {
					LOG.error(StringUtils.stringifyException(ioe));
				}
			}
		}
	}

	void start() {

		this.serverThread.start();

		if (LOG.isInfoEnabled()) {
			LOG.info("Started BLOB manager on " + this.serverSocket.getLocalSocketAddress());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	URL getURL(final BlobKey key) throws IOException {

		final File blob = getLocal(key);
		if (blob == null) {
			return null;
		}

		return blob.toURI().toURL();
	}
}
