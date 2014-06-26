package eu.stratosphere.nephele.services.blob;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.Random;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobID;

abstract class AbstractBaseImpl {

	protected static final String BLOB_FILE_PREFIX = "blob_";

	protected final File storageDirectory;

	private final Random rnd = new Random();

	protected AbstractBaseImpl() throws IOException {

		this.storageDirectory = createStorageDirectory();
	}

	abstract BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException;

	abstract BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException;

	abstract InputStream get(final BlobKey key) throws IOException;

	abstract URL getURL(final BlobKey key) throws IOException;

	/**
	 * Shuts down the BLOB service
	 */
	void shutdown() {

		// Wipe the storage directory
		deleteStorageDirectory();
	}

	private void deleteStorageDirectory() {

		final File[] files = this.storageDirectory.listFiles();
		for (final File file : files) {

			if (file.getName().startsWith(BLOB_FILE_PREFIX)) {
				file.delete();
			}
		}

		this.storageDirectory.delete();
	}

	private static File createStorageDirectory() {

		final String dir = GlobalConfiguration.getString(ConfigConstants.BLOB_SERVICE_DIRECTORY, null);
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
	 * Converts the key of a BLOB into it's designated storage location.
	 * 
	 * @param key
	 *        the key of the BLOB
	 * @return the BLOB's designated storage location
	 */
	protected File keyToFilename(final BlobKey key) {

		return new File(this.storageDirectory, BLOB_FILE_PREFIX + key.toString());
	}

	/**
	 * Checks if the BLOB with the given key exists in the local BLOB cache.
	 * 
	 * @param key
	 *        the key of the BLOB to return
	 * @return the filename of the BLOB with the given key, <code>null</code> if it could not be found
	 */
	protected File getLocal(final BlobKey key) {

		final File file = keyToFilename(key);
		if (file.exists()) {
			return file;
		}

		return null;
	}
}
