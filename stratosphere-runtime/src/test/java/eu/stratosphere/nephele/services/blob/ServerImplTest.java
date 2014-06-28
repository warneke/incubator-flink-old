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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class contains unit tests for the BLOB service's server component.
 */
public final class ServerImplTest {

	/**
	 * The size of the test file used by the unit tests.
	 */
	private static final int SIZE_OF_TEST_FILE = 16 * 1024;

	/**
	 * The socket address the test server shall bind to.
	 */
	private static final InetSocketAddress SERVER_ADDR = new InetSocketAddress(
			ConfigConstants.DEFAULT_BLOB_SERVICE_PORT);

	/**
	 * The job ID used by the unit tests.
	 */
	private static final JobID JOB_ID = JobID.generate();

	/**
	 * The generated test file.
	 */
	private static File TEST_FILE;

	/**
	 * The key of the generated test file.
	 */
	private static BlobKey TEST_KEY;

	/**
	 * Creates the test file for the unit tests and computes its BLOB key.
	 */
	@BeforeClass
	public static void createTestFile() {

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[4096];
		buf[0] = 1;
		buf[1] = 2;
		buf[2] = 3;

		FileOutputStream fos = null;
		try {
			TEST_FILE = File.createTempFile("test", ".dat");
			fos = new FileOutputStream(TEST_FILE);
			int bytesToWrite = SIZE_OF_TEST_FILE;
			while (bytesToWrite > 0) {
				final int len = Math.min(bytesToWrite, buf.length);
				md.update(buf, 0, len);
				fos.write(buf, 0, len);
				bytesToWrite -= len;
			}

			fos.close();
			fos = null;

			TEST_KEY = new BlobKey(md.digest());

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} finally {
			closeSilently(fos);
		}
	}

	/**
	 * Initializes the server component of the BLOB service.
	 */
	@BeforeClass
	public static void startServer() {

		try {
			BlobService.initServer(SERVER_ADDR);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}
	}

	/**
	 * Stops the server component of the BLOB service.
	 */
	@AfterClass
	public static void stopServer() {

		BlobService.shutdown();
	}

	/**
	 * Deletes the test file.
	 */
	@AfterClass
	public static void deleteTestFile() {

		if (TEST_FILE != null) {
			TEST_FILE.delete();
		}
	}

	/**
	 * Attempts to retrieve the BLOB with the given key through a stateful get
	 * operation and verifies the integrity of its content.
	 * 
	 * @param key
	 *            the key of the BLOB to retrieve
	 */
	private void verifyStatefulGet(final BlobKey key) {

		final byte[] buf = new byte[1024];
		final MessageDigest md = BlobService.getMessageDigest();

		InputStream is = null;
		try {

			is = BlobService.get(key);

			while (true) {

				final int read = is.read(buf);
				if (read < 0) {
					break;
				}

				md.update(buf, 0, read);
			}

			assertEquals(key, new BlobKey(md.digest()));

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} finally {
			closeSilently(is);
		}
	}

	/**
	 * Attempts to retrieve the BLOB with the given key through a stateless get
	 * operation and verifies the integrity of its content.
	 * 
	 * @param key
	 *            the key of the BLOB to retrieve
	 */
	private void verifyStatelessGet(final BlobKey key) {

		final byte[] buf = new byte[1024];
		final MessageDigest md = BlobService.getMessageDigest();

		InputStream is = null;
		try {

			is = BlobService.get(key, SERVER_ADDR);

			while (true) {

				final int read = is.read(buf);
				if (read < 0) {
					break;
				}

				md.update(buf, 0, read);
			}

			assertEquals(key, new BlobKey(md.digest()));

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} finally {
			closeSilently(is);
		}
	}

	/**
	 * Tests storing and retrieving a byte buffer through a stateful put/get
	 * operation.
	 */
	@Test
	public void testStatefulByteBufferPutAndGet() {

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[578];
		buf[0] = 1;
		buf[1] = 2;
		buf[2] = 3;
		md.update(buf);
		final BlobKey expectedKey = new BlobKey(md.digest());

		// Test stateful put
		try {
			final BlobKey key = BlobService.put(JOB_ID, buf, 0, buf.length);
			assertEquals(expectedKey, key);

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		// Test stateful get
		verifyStatefulGet(expectedKey);
	}

	/**
	 * Tests storing and retrieving the data from an input stream through a
	 * stateful put/get operation.
	 */
	@Test
	public void testStatefulInputStreamPutAndGet() {

		// Test stateful put
		InputStream is = null;
		try {
			is = new FileInputStream(TEST_FILE);
			final BlobKey key = BlobService.put(JOB_ID, is);
			assertEquals(TEST_KEY, key);

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} finally {
			closeSilently(is);
		}

		// Test stateful get
		verifyStatefulGet(TEST_KEY);
	}

	/**
	 * Tests storing and retrieving a byte buffer through a stateless put/get
	 * operation.
	 */
	@Test
	public void testStatelessByteBufferPutAndGet() {

		final MessageDigest md = BlobService.getMessageDigest();
		final byte[] buf = new byte[578];
		buf[0] = 1;
		buf[1] = 2;
		buf[2] = 3;
		md.update(buf);
		final BlobKey expectedKey = new BlobKey(md.digest());

		// Test stateless put
		try {
			final BlobKey key = BlobService.put(JOB_ID, buf, 0, buf.length,
					SERVER_ADDR);
			assertEquals(expectedKey, key);

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		// Test stateless get
		verifyStatelessGet(expectedKey);
	}

	/**
	 * Tests storing and retrieving the data from an input stream through a
	 * stateless put/get operation.
	 */
	@Test
	public void testStatelessInputStreamPutAndGet() {

		// Test stateless put
		InputStream is = null;
		try {
			is = new FileInputStream(TEST_FILE);
			final BlobKey key = BlobService.put(JOB_ID, is, SERVER_ADDR);
			assertEquals(TEST_KEY, key);

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} finally {
			closeSilently(is);
		}

		// Test stateless get
		verifyStatelessGet(TEST_KEY);
	}

	/**
	 * Tests the response of the BLOB service (stateless mode) to an invalid
	 * key.
	 */
	@Test
	public void testStatelessGetWithInvalidKey() {

		try {
			BlobService.get(new BlobKey(), SERVER_ADDR);
		} catch (FileNotFoundException fnfe) {
			return;
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		fail("Expected FileNotFoundException");
	}

	/**
	 * Tests the response of the BLOB service (stateful mode) to an invalid key.
	 */
	@Test
	public void testStatefulGetWithInvalidKey() {

		try {
			BlobService.get(new BlobKey());
		} catch (FileNotFoundException fnfe) {
			return;
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		fail("Expected FileNotFoundException");
	}

	/**
	 * Auxiliary method to silently close a {@link Closeable} object.
	 * 
	 * @param closeable
	 *            the object to close
	 */
	private static void closeSilently(final Closeable closeable) {

		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException ioe) {
			}
		}
	}
}
