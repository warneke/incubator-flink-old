package eu.stratosphere.nephele.services.blob;

import java.io.IOException;

import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;

final class BlobTempFile {

	private final Path path;

	private final FSDataOutputStream outputStream;

	BlobTempFile(final Path path, final FSDataOutputStream outputStream) {
		this.path = path;
		this.outputStream = outputStream;
	}

	Path getPath() {
		return this.path;
	}

	FSDataOutputStream getOutputStream() {
		return this.outputStream;
	}

	void delete() {

		try {
			this.outputStream.close();
		} catch (IOException ioe) {
		}

		try {
			final FileSystem fs = this.path.getFileSystem();
			fs.delete(this.path, false);
		} catch (IOException e) {
		}
	}
}
