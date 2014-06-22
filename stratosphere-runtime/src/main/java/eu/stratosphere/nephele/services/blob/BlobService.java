package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.io.InputStream;

import eu.stratosphere.core.fs.FSDataInputStream;

public final class BlobService {

	private BlobService() {
	}

	public BlobKey put(final byte[] buffer) throws IOException {

		return null;
	}

	public BlobKey put(final InputStream inputStream) throws IOException {

		return null;
	}

	public BlobKey put(final FSDataInputStream inputStream) throws IOException {

		return null;
	}
}
