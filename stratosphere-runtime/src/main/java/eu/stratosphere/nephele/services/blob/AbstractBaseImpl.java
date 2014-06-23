package eu.stratosphere.nephele.services.blob;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobID;

abstract class AbstractBaseImpl {

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	protected final File storageDirectory;

	private final Random rnd = new Random();

	protected AbstractBaseImpl() throws IOException {

		this.storageDirectory = createStorageDirectory();
	}

	abstract BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException;

	abstract BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException;

	abstract InputStream get(final BlobKey key) throws IOException;

	abstract void shutdown();

	private static File createStorageDirectory() {

		final String dir = GlobalConfiguration.getString(ConfigConstants.BLOB_MANAGER_DIRECTORY, null);
		final File storageBaseDir = new File((dir != null) ? dir : System.getProperty("java.io.tmpdir"));

		// Determine user name
		String userName = System.getProperty("user.name");
		if (userName == null) {
			userName = "default";
		}

		// Determine the process ID
		final int pid = getProcessID();

		// Construct storage directory based on user name and PID, afterwards create it
		final File storageDirectory = new File(storageBaseDir, String.format("blob-%s-%d", userName, pid));
		storageDirectory.mkdirs();

		return storageDirectory;
	}

	protected File createTempFile() throws IOException {

		synchronized (this.rnd) {

			while (true) {

				final int r = this.rnd.nextInt(10000);
				final File tmpFile = new File(this.storageDirectory, String.format("tmp-%d", r));
				if (!tmpFile.exists()) {
					return tmpFile;
				}
			}
		}
	}

	private static int getProcessID() {

		try {

			final String name = ManagementFactory.getRuntimeMXBean().getName();
			if (name == null) {
				return 0;
			}

			final String[] fields = name.split("@");

			return Integer.parseInt(fields[0]);

		} catch (Exception e) {
		}

		return 0;
	}

	/**
	 * Returns an instance of the message digest to use for the BLOB key computation.
	 * 
	 * @return an instance of the message digest to use for the BLOB key computation
	 */
	protected static MessageDigest getMessageDigest() {

		try {
			return MessageDigest.getInstance(HASHING_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	protected static void writeLength(final int length, final byte[] buf, final OutputStream outputStream)
			throws IOException {

		buf[0] = (byte) (length & 0xff);
		buf[1] = (byte) ((length >> 8) & 0xff);
		buf[2] = (byte) ((length >> 16) & 0xff);
		buf[3] = (byte) ((length >> 24) & 0xff);

		outputStream.write(buf);
	}

	protected static int readLength(final byte[] buf, final InputStream inputStream) throws IOException {

		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		bytesRead = buf[0] & 0xff;
		bytesRead |= (buf[1] & 0xff) << 8;
		bytesRead |= (buf[2] & 0xff) << 16;
		bytesRead |= (buf[3] & 0xff) << 24;

		return bytesRead;
	}

	protected static void sendJobID(final JobID jobID, final OutputStream outputStream) throws IOException {

		if (jobID == null) {
			// Write 0 to indicate no job ID is following
			outputStream.write(0);
			return;
		}

		// Write 1 to indicate a job ID is following
		outputStream.write(1);
		final byte[] buf = new byte[JobID.SIZE];
		final ByteBuffer bb = ByteBuffer.wrap(buf);
		jobID.write(bb);
		outputStream.write(buf);
	}

	protected static JobID receiveJobID(final InputStream inputStream) throws IOException {

		int read = inputStream.read();
		if (read < 0) {
			throw new EOFException();
		} else if (read == 0) {
			return null;
		}

		final byte[] buf = new byte[JobID.SIZE];
		int bytesRead = 0;
		while (bytesRead < JobID.SIZE) {
			read = inputStream.read(buf, bytesRead, JobID.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		return new JobID(buf);
	}

	protected File getLocal(final BlobKey key) throws IOException {

		final File file = new File(this.storageDirectory, key.toString());
		if (file.exists()) {
			return file;
		}

		return null;
	}
}
