/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

/**
 * This class implements the server component of the BLOB service. Besides the basic file cache it serves put and get
 * requests to clients over the network.
 * <p>
 * This class is thread-safe.
 */
final class ServerImpl extends AbstractBaseImpl implements Runnable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ServerImpl.class);

	/**
	 * The server socket the BLOB manager listens on for incoming connections.
	 */
	private final ServerSocket serverSocket;

	/**
	 * The thread listening for incoming network connections.
	 */
	private final Thread serverThread;

	/**
	 * Indicates whether a shutdown of server component has been requested.
	 */
	private volatile boolean shutdownRequested = false;

	/**
	 * Constructs a new server component of the BLOB service.
	 * 
	 * @param socketAddress
	 *        the socket address the server shall listen on
	 * @throws IOException
	 *         thrown if an error occurs while binding/initializing the server socket
	 */
	ServerImpl(final InetSocketAddress socketAddress) throws IOException {
		super();

		this.serverSocket = new ServerSocket();
		this.serverSocket.bind(socketAddress);

		this.serverThread = new Thread(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		final MessageDigest md = BlobService.getMessageDigest();
		File tempFile = null;
		FileOutputStream fos = null;

		try {

			tempFile = createTempFile();
			fos = new FileOutputStream(tempFile);

			md.update(buf, offset, len);
			fos.write(buf, offset, len);

			// Close file stream and compute key
			fos.close();
			fos = null;
			final BlobKey key = new BlobKey(md.digest());
			tempFile.renameTo(keyToFilename(key));
			tempFile = null;

			return key;

		} finally {
			if (fos != null) {
				fos.close();
			}
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];
		File tempFile = null;
		FileOutputStream fos = null;

		try {

			tempFile = createTempFile();
			fos = new FileOutputStream(tempFile);

			while (true) {

				final int read = inputStream.read(buf);
				if (read < 0) {
					break;
				}

				md.update(buf);
				fos.write(buf);
			}

			// Close file stream and compute key
			fos.close();
			fos = null;
			final BlobKey key = new BlobKey(md.digest());
			tempFile.renameTo(keyToFilename(key));
			tempFile = null;

			return key;

		} finally {
			if (fos != null) {
				fos.close();
			}
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}

	/**
	 * Stores data in a BLOB from an incoming network connection.
	 * 
	 * @param inputStream
	 *        the input stream to read the data from
	 * @return the key of stored BLOB
	 * @throws IOException
	 *         throw if an I/O error occurs during the data transfer
	 */
	BlobKey putFromNetwork(final InputStream inputStream) throws IOException {

		BlobService.receiveJobID(inputStream);

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];
		final byte[] lenBuf = new byte[4];
		File tempFile = null;
		FileOutputStream fos = null;
		try {

			tempFile = createTempFile();
			fos = new FileOutputStream(tempFile);

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
				fos.write(buf, 0, bytesInBuffer);
			}

			// Close file stream and compute key
			fos.close();
			fos = null;
			final BlobKey key = new BlobKey(md.digest());
			tempFile.renameTo(keyToFilename(key));
			tempFile = null;

			return key;

		} finally {
			if (fos != null) {
				fos.close();
			}
			if (tempFile != null) {
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
		if (blob == null) {
			throw new FileNotFoundException();
		}

		return new FileInputStream(blob);
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

	/**
	 * Starts the thread listening for incoming network connections.
	 */
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
			throw new FileNotFoundException();
		}

		return blob.toURI().toURL();
	}
}
