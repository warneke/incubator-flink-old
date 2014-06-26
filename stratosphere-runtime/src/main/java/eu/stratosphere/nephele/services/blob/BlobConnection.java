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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.util.StringUtils;

/**
 * This class handles incoming network connections for the BLOB service's server component.
 * <p>
 * This class is thread-safe.
 */
final class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	/**
	 * The BLOB service's server component.
	 */
	private final ServerImpl blobServer;

	/**
	 * The socket to use for communication.
	 */
	private final Socket socket;

	/**
	 * Constructs a new connection object.
	 * 
	 * @param blobServer
	 *        the BLOB service's server component
	 * @param socket
	 *        the socket to use for communication
	 */
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

	/**
	 * Handles a client's put request.
	 * 
	 * @param inputStream
	 *        the input stream of the socket
	 * @param outputStream
	 *        the output stream of the socket
	 * @throws IOException
	 *         thrown if an I/O error occurs during the data transfer
	 */
	private void put(final InputStream inputStream, final OutputStream outputStream) throws IOException {

		final BlobKey key = this.blobServer.putFromNetwork(inputStream);

		// Send the key back to the client for verification
		key.writeToOutputStream(outputStream);
	}

	/**
	 * Handles a client's get request.
	 * 
	 * @param inputStream
	 *        the input stream of the socket
	 * @param outputStream
	 *        the output stream of the socket
	 * @throws IOException
	 *         thrown if an I/O error occurs during the data transfer
	 */
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
