package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;

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

		final File blob = getLocal(key);
		if (blob != null) {
			return new FileInputStream(blob);
		}

		return getFromServer(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void shutdown() {
		// TODO Wipe the storage directory
	}

	private InputStream getFromServer(final BlobKey key) throws IOException {

		Socket socket = null;
		int status = 0;
		try {
			socket = new Socket(this.serverAddress.getAddress(), this.serverAddress.getPort());
			final OutputStream os = socket.getOutputStream();
			os.write(BlobService.GET_OPERATION);
			key.writeToOutputStream(os);
			os.flush();

			final InputStream is = socket.getInputStream();
			status = is.read();
			if (status < 0) {
				throw new EOFException();
			} else if (status == 0) {
				throw new FileNotFoundException();
			}

			return is;

		} finally {
			if (status <= 0) {
				BlobService.closeSilently(socket);
			}
		}
	}

	@Override
	URL getURL(BlobKey key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
