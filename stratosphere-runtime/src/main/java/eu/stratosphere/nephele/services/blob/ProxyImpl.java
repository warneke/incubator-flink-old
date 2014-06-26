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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.MessageDigest;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements the proxy component of the BLOB service. The proxy component first tries to server get requests
 * from the local cache and then tries to download the requested BLOBs from the server. Put requests are directly
 * forwarded to the server.
 * <p>
 * This class is thread-safe.
 */
final class ProxyImpl extends AbstractBaseImpl {

	/**
	 * The socket address of the server component.
	 */
	private final InetSocketAddress serverAddress;

	/**
	 * Constructs a new proxy component of the BLOB service.
	 * 
	 * @param serverAddress
	 *        the socket address of the server component
	 */
	ProxyImpl(final InetSocketAddress serverAddress) {
		super();

		this.serverAddress = serverAddress;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		return BlobService.put(jobID, buf, offset, len, this.serverAddress);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {

		return BlobService.put(jobID, inputStream, this.serverAddress);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	InputStream get(final BlobKey key) throws IOException {

		// Check if the BLOB is cached
		File blob = getLocal(key);
		if (blob != null) {
			return new FileInputStream(blob);
		}

		// Try to download the BLOB from the server
		fetchFromServer(key);

		// Try, again after the download
		blob = getLocal(key);
		if (blob != null) {
			return new FileInputStream(blob);
		}

		throw new FileNotFoundException();
	}

	/**
	 * Downloads the BLOB with the given key from the server component to the local BLOB cache.
	 * 
	 * @param key
	 *        the key of the BLOB to download
	 * @throws IOException
	 *         thrown if an I/O error or data corruption occurs during the data transfer
	 */
	private void fetchFromServer(final BlobKey key) throws IOException {

		final InputStream inputStream = BlobService.get(key, this.serverAddress);

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
			if (!key.equals(new BlobKey(md.digest()))) {
				throw new IOException("Detected data corruption during transfer");
			}

			tempFile.renameTo(keyToFilename(key));
			tempFile = null;

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
	URL getURL(final BlobKey key) throws IOException {

		// Check if the BLOB is cached
		File blob = getLocal(key);
		if (blob != null) {
			return blob.toURI().toURL();
		}

		// Try to download the BLOB from the server
		fetchFromServer(key);

		// Try, again after the download
		blob = getLocal(key);
		if (blob != null) {
			return blob.toURI().toURL();
		}

		throw new FileNotFoundException();
	}
}
