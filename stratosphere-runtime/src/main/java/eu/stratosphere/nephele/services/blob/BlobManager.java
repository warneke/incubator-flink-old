package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

public final class BlobManager extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobManager.class);

	private final Random rnd = new Random();

	/**
	 * The server socket the BLOB manager listens on for incoming connections.
	 */
	private final ServerSocket serverSocket;

	private final Path storageDirectory;

	private final FileSystem storageFileSystem;

	private final Map<BlobKey, Set<JobID>> blobToJobMap = new HashMap<BlobKey, Set<JobID>>();

	/**
	 * Indicates whether a shutdown of the BLOB manager has been requested.
	 */
	private volatile boolean shutdownRequested = false; // TODO: Reconsider this design

	public BlobManager(final InetAddress address, final int port) throws IOException {
		this(new InetSocketAddress(address, port));
	}

	public BlobManager(final InetSocketAddress socketAddress) throws IOException {
		super("BLOB manager");

		this.storageDirectory = createStorageDirectory();
		this.storageFileSystem = this.storageDirectory.getFileSystem();

		this.serverSocket = new ServerSocket();
		this.serverSocket.bind(socketAddress);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info("Started BLOB manager on "
				+ this.serverSocket.getLocalSocketAddress());
		}
	}

	private static final Path createStorageDirectory() throws IOException {

		String dir = GlobalConfiguration.getString(ConfigConstants.BLOB_STORAGE_DIRECTORY, null);
		final Path storageBaseDir;
		final FileSystem fs;
		if (dir != null) {
			storageBaseDir = new Path(dir);
			fs = storageBaseDir.getFileSystem();
		} else {
			fs = FileSystem.getLocalFileSystem();
			storageBaseDir = new Path(new Path(fs.getUri()), System.getProperty("java.io.tmpdir"));
		}

		// Determine user name
		String userName = System.getProperty("user.name");
		if (userName == null) {
			userName = "default";
		}

		// Determine the process ID
		final int pid = getProcessID();

		// Construct storage directory based on user name and PID, afterwards create it
		final Path storageDirectory = new Path(storageBaseDir, String.format("blob-%s-%d", userName, pid));
		fs.mkdirs(storageDirectory);

		return storageDirectory;
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

	synchronized BlobTempFile createTempFile() throws IOException {

		while (true) {

			final int r = this.rnd.nextInt(10000);
			final Path tmpFile = new Path(this.storageDirectory, String.format("tmp-%d", r));
			if (!this.storageFileSystem.exists(tmpFile)) {
				final FSDataOutputStream os = this.storageFileSystem.create(tmpFile, false);
				return new BlobTempFile(tmpFile, os);
			}
		}
	}

	synchronized boolean put(final JobID jobID, final BlobKey key, final BlobTempFile tempFile) throws IOException {

		Set<JobID> jobs = this.blobToJobMap.get(key);
		if (jobs == null) {
			jobs = new HashSet<JobID>();
			this.blobToJobMap.put(key, jobs);
		}
		jobs.add(jobID);

		final Path keyPath = new Path(this.storageDirectory, key.toString());
		if (this.storageFileSystem.exists(keyPath)) {
			return false;
		}

		this.storageFileSystem.rename(tempFile.getPath(), keyPath);

		return true;
	}

	synchronized InputStream get(final BlobKey key) throws IOException {

		final Path keyPath = new Path(this.storageDirectory, key.toString());
		if (!this.storageFileSystem.exists(keyPath)) {
			return null;
		}

		return this.storageFileSystem.open(keyPath);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!this.shutdownRequested) {

			try {

				final Socket socket = this.serverSocket.accept();
				new BlobConnection(this, socket).start();

			} catch (IOException ioe) {
				if (!this.shutdownRequested && LOG.isErrorEnabled()) {
					LOG.error(StringUtils.stringifyException(ioe));
				}
			}
		}
	}

	/**
	 * Shuts down the BLOB manager.
	 */
	public void shutdown() {

		this.shutdownRequested = true;
		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ioe));
			}
		}

		try {
			join();
		} catch (InterruptedException ie) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(ie));
			}
		}
	}

}
