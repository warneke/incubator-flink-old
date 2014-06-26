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
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.Random;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This is the base class of the BLOB service's stateful components. The class implements basic functionality for a file
 * cache and declares the operations the BLOB service supports.
 * <p>
 * This class is thread-safe.
 */
abstract class AbstractBaseImpl {

	/**
	 * The file prefix for all BLOBs stored by the BLOB service.
	 */
	protected static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The directory to store all the BLOBs in.
	 */
	protected final File storageDirectory;

	/**
	 * A random number generated used to create unique temporary filenames.
	 */
	private final Random rnd = new Random();

	/**
	 * Constructs a new object and initializes the BLOB storage directory.
	 */
	protected AbstractBaseImpl() {

		this.storageDirectory = createStorageDirectory();
	}

	/**
	 * Stores the given byte buffer.
	 * 
	 * @param jobID
	 *        the ID of the job the byte buffer belongs to
	 * @param buf
	 *        the byte buffer to store
	 * @param offset
	 *        the offset in the byte buffer
	 * @param len
	 *        the number of bytes to read from the byte buffer
	 * @return the BLOB key identifying the stored byte buffer
	 * @throws IOException
	 *         thrown if an I/O error occurs while storing the data
	 */
	abstract BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException;

	/**
	 * Reads the data from the given input stream and stores it.
	 * 
	 * @param jobID
	 *        the ID of the job the data to store belong to
	 * @param inputStream
	 *        the input stream to read the data from
	 * @return the BLOB key identifying the stored data
	 * @throws IOException
	 *         thrown if an I/O error occurs while storing the data
	 */
	abstract BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException;

	/**
	 * Loads the data from the BLOB with the given key.
	 * 
	 * @param key
	 *        the key of the BLOB to read
	 * @return an input stream to the BLOB's data
	 * @throws FileNotFoundException
	 *         thrown if no BLOB with the given key could be found
	 * @throws IOException
	 *         throw if an I/O error occurs while retrieving the data
	 */
	abstract InputStream get(final BlobKey key) throws IOException;

	/**
	 * Returns the URL of the BLOB with the given key
	 * 
	 * @param key
	 *        the key of the requested BLOB
	 * @return the URL of the BLOB
	 * @throws FileNotFoundException
	 *         thrown if no BLOB with the given key could be found
	 * @throws IOException
	 *         throw if an I/O error occurs while retrieving the data
	 */
	abstract URL getURL(final BlobKey key) throws IOException;

	/**
	 * Shuts down the BLOB service.
	 */
	void shutdown() {

		// Wipe the storage directory
		deleteStorageDirectory();
	}

	/**
	 * Deletes all BLOB files from the storage directory. If the directory is empty thereafter, it is deleted as well.
	 */
	private void deleteStorageDirectory() {

		final File[] files = this.storageDirectory.listFiles();
		for (final File file : files) {

			if (file.getName().startsWith(BLOB_FILE_PREFIX)) {
				file.delete();
			}
		}

		this.storageDirectory.delete();
	}

	/**
	 * Creates a storage directory for the BLOB service.
	 * 
	 * @return the storage directory for the BLOB service
	 */
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

	/**
	 * Creates a temporary file in the storage directory. The name of the temporary file is guaranteed to be unique.
	 * 
	 * @return a temporary file in the storage directory
	 */
	protected File createTempFile() {

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

	/**
	 * Returns the operating system process ID of the current JVM.
	 * 
	 * @return the operating system process ID of the current JVM or 0 if the ID could not be determined
	 */
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
