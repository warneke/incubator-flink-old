package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

final class BlobConnection extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobConnection.class);

	private final BlobManager blobManager;

	private final Socket socket;

	BlobConnection(final BlobManager blobManager, final Socket socket) {
		super("BLOB connection from " + socket.getRemoteSocketAddress());

		this.blobManager = blobManager;
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

		final JobID jobID = BlobService.receiveJobID(inputStream);

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[BlobService.TRANSFER_BUFFER_SIZE];
		final byte[] lenBuf = new byte[4];

		final BlobTempFile tempFile = this.blobManager.createTempFile();
		final FSDataOutputStream fos = tempFile.getOutputStream();
		boolean deleteTempFile = true;
		try {

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
			final BlobKey key = new BlobKey(md.digest());

			deleteTempFile = !this.blobManager.put(jobID, key, tempFile);

			// TODO: Register BLOB with BLOB manager

			// Send the key back to the client for verification
			key.writeToOutputStream(outputStream);

		} finally {
			if (deleteTempFile) {
				tempFile.delete();
			}
		}

	}

	private void get(final InputStream inputStream, final OutputStream outputStream) throws IOException {

		// Receive the blob key
		final BlobKey key = BlobKey.readFromInputStream(inputStream);

		final InputStream fis = this.blobManager.get(key);
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
