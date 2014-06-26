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
