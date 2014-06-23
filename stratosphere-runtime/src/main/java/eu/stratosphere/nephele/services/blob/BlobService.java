package eu.stratosphere.nephele.services.blob;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class BlobService {

	static final int TRANSFER_BUFFER_SIZE = 4096;

	static final byte PUT_OPERATION = 0;

	static final byte GET_OPERATION = 1;

	private static final AtomicReference<AbstractBaseImpl> BLOB_SERVICE_IMPL = new AtomicReference<AbstractBaseImpl>(
		null);

	private BlobService() {
	}

	public static void initProxy(final InetSocketAddress serverAddress) throws IOException {

		while (true) {

			if (BLOB_SERVICE_IMPL.get() != null) {
				return;
			}

			final ProxyImpl proxyImpl = new ProxyImpl(serverAddress);
			if (BLOB_SERVICE_IMPL.compareAndSet(null, proxyImpl)) {
				return;
			}
		}
	}

	public static void initServer(final InetSocketAddress socketAddress) throws IOException {

		while (true) {

			if (BLOB_SERVICE_IMPL.get() != null) {
				return;
			}

			final ServerImpl serverImpl = new ServerImpl(socketAddress);
			if (BLOB_SERVICE_IMPL.compareAndSet(null, serverImpl)) {
				serverImpl.start();
				return;
			}
		}
	}

	private static AbstractBaseImpl get() {

		final AbstractBaseImpl impl = BLOB_SERVICE_IMPL.get();
		if (impl == null) {
			throw new IllegalStateException("BLOB service has not been initalized yet");
		}

		return impl;
	}

	public static BlobKey put(final JobID jobID, final byte[] buf) throws IOException {

		return get().put(jobID, buf, 0, buf.length);
	}

	public static BlobKey put(final JobID jobID, final byte[] buf, final int offset, final int len) throws IOException {

		return get().put(jobID, buf, offset, len);
	}

	public static BlobKey put(final JobID jobID, final InputStream inputStream) throws IOException {

		return get().put(jobID, inputStream);
	}

	public static InputStream get(final BlobKey key) throws IOException {

		return get().get(key);
	}

	public static void shutdown() {

		get().shutdown();
	}
}
