package com.nonblockingserver;

import com.github.naturs.logger.adapter.DefaultLogAdapter;
import com.github.naturs.logger.strategy.format.PrettyFormatStrategy;
import com.nonblockingserver.util.InternetUtil;
import com.zhongyou.jobschedule.JobSchedule;
import com.zhongyou.protocol.parser.AbsParserBuffer;
import com.zhongyou.util.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SocketChannelOperator {
	private final int mBufferSize;
	private final ByteBuffer mIOBuffer;
	private final byte[] mBackBuffer;
	private SocketChannel mChannel;
	private final AbsParserBuffer mInputBuffer;
	private final AbsParserBuffer mOutputBuffer;
	private SelectorHandler mSelectorHandler;

	private static final String TAG = SocketChannelOperator.class.getSimpleName();

	private AtomicInteger mStatusValue = new AtomicInteger(Status.Idle.ordinal());
	private StatusListener mStatusListener;

	private Lock mCriticalSectionLock = new ReentrantLock();

	public static void main(String... args) throws IOException, InterruptedException {
		Logger.addLogAdapter(new DefaultLogAdapter(PrettyFormatStrategy.newBuilder().build()));
		NonBlockingServer server = new NonBlockingServer();
		server.asyncServer(InternetUtil.getLocalHostLANAddress());
		server.waitForStart();
		SocketChannelOperator operator = new SocketChannelOperator(8000) {
			private byte[] buffer = new byte[8000];

			@Override
			protected void afterHandleIO() {
				if (getCachedInputBulkDataCount() > 0) {
					server.getJobScheduler().requestJobSchedule(new JobSchedule(this::handleTasks, 0L));
				}
			}

			private void handleTasks() {
				int read;
				while ((read = readCachedBulkData(buffer, 0)) > 0) {
					try {
						String toString = new String(buffer, 0, read);
						Logger.d(toString);
						byte[] data = ("Send:" + toString).getBytes();
						sendBulkData(data, 0, data.length);
					} catch (Exception e) {
						Logger.printException(e);
					}
				}
			}
		};
		operator.open(server.getSelectorHandler(), new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 12345));
	}

	public void setStatusListener(StatusListener statusListener) {
		mStatusListener = statusListener;
	}

	public enum Status {
		Idle,
		PrepareConnection,
		ConnectionPending,
		HandlePendingConnection,
		Established,
		HandleEstablishedConnection,
		PrepareDisconnection,
		Disconnected
	}

	public Status getStatus() {
		return Status.values()[mStatusValue.get()];
	}

	public interface StatusListener {
		void onStatusUpdate(Status update);
	}

	private void update(Status update) {
		mStatusValue.set(update.ordinal());
		StatusListener listener = mStatusListener;
		if (listener != null) {
			listener.onStatusUpdate(update);
		}
	}

	private final Consumer<SelectionKey> mSelectionProcessor = key -> {
		beforeHandleIO();
		try {
			mCriticalSectionLock.lock();
			Status status = getStatus();
			switch (status) {
				case ConnectionPending:
					update(Status.HandlePendingConnection);
					if (mChannel.isConnectionPending()) {
						try {
							mChannel.finishConnect();
							mSelectorHandler.getSelectorProxy().updateInterestOps(mChannel, SelectionKey.OP_READ);
							update(Status.Established);
						} catch (IOException e) {
							update(Status.ConnectionPending);
							Logger.printException(e);
						}
					} else {
						update(Status.ConnectionPending);
					}
					break;
				case Established:
					update(Status.HandleEstablishedConnection);
					if (key.isReadable()) {
						try {
							int read;
							do {
								read = receive(key);
							}
							while (read > 0);
						} catch (EOFException e) {
							update(Status.PrepareDisconnection);
							innerCloseOperations();
							Logger.d("TCP Socket closed");
							update(Status.Disconnected);
							break;
						} catch (IOException e) {
							Logger.printException(e);
						}
					}
					if (key.isWritable()) {
						try {
							AbsParserBuffer outputBuffer = getOutputBuffer();
							if (outputBuffer.getCachedBytes() > 0) {
								send(outputBuffer);
							}
							mSelectorHandler.getSelectorProxy().updateInterestOps(mChannel, mSelectorHandler.getSelectorProxy().interestOps(mChannel) & (~SelectionKey.OP_WRITE));
						} catch (IOException e) {
							Logger.printException(e);
						}
					}
					update(Status.Established);
					break;
				default:
					break;
			}
		} finally {
			mCriticalSectionLock.unlock();
		}
		afterHandleIO();
	};


	public SocketChannelOperator(int bufferSize) {
		mBufferSize = bufferSize;
		mIOBuffer = ByteBuffer.allocate(mBufferSize);
		mBackBuffer = new byte[mBufferSize];
		mInputBuffer = new AbsParserBuffer(bufferSize);
		mOutputBuffer = new AbsParserBuffer(bufferSize);
	}

	private Consumer<SelectionKey> getSelectionKeyProcessor() {
		return mSelectionProcessor;
	}

	public boolean open(SelectorHandler selectorHandler, InetSocketAddress dstAddress) throws IOException {
		if (selectorHandler == null) {
			throw new NullPointerException("Null SelectorHandler specification ");
		}
		try {
			mCriticalSectionLock.lock();
			Status status = getStatus();
			switch (status) {
				case Idle:
				case Disconnected:
					break;
				default:
					Logger.d(TAG, String.format("Open %s on illegal status %s", SocketChannelOperator.class.getSimpleName(), status));
					return false;
			}
			update(Status.PrepareConnection);
			SocketChannel socketChannel = null;
			try {
				socketChannel = SocketChannel.open();
				socketChannel.configureBlocking(false);
				socketChannel.connect(dstAddress);
			} catch (Exception e) {
				update(status);
				if (socketChannel != null && socketChannel.isOpen()) {
					try {
						socketChannel.close();
					} catch (Exception e2) {
						//do nothing
					}
				}
				throw e;
			}
			try {
				selectorHandler.registerSelectionKeyProcessor(socketChannel, getSelectionKeyProcessor());
				selectorHandler.getSelectorProxy().updateInterestOps(socketChannel, SelectionKey.OP_CONNECT);
			} catch (Exception e) {
				update(status);
				selectorHandler.unregisterSelectionKeyProcessor(socketChannel);
				try {
					socketChannel.close();
				} catch (Exception e2) {
					//do nothing
				}
				throw e;
			}
			mChannel = socketChannel;
			mSelectorHandler = selectorHandler;
			update(Status.ConnectionPending);
			return true;
		} finally {
			mCriticalSectionLock.unlock();
		}
	}

	public boolean close() {
		try {
			mCriticalSectionLock.lock();
			Status status = getStatus();
			switch (status) {
				case Idle:
				case Disconnected:
					return true;
				case Established:
				case ConnectionPending:
					break;
				default:
					return false;
			}
			update(Status.PrepareDisconnection);
			innerCloseOperations();
			Logger.d("TCP Socket closed");
			update(Status.Disconnected);
			return true;
		} finally {
			mCriticalSectionLock.unlock();
		}
	}

	public final boolean sendBulkData(byte[] bulkData, int offset, int length) {
		try {
			mCriticalSectionLock.lock();
			switch (getStatus()) {
				case Established:
					break;
				default:
					return false;
			}
			mOutputBuffer.offer(bulkData, offset, length);
			int interestOptNow = mSelectorHandler.getSelectorProxy().interestOps(mChannel);
			if ((interestOptNow & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
				mSelectorHandler.getSelectorProxy().updateInterestOps(mChannel, interestOptNow | SelectionKey.OP_WRITE);
			}
			return true;
		} finally {
			mCriticalSectionLock.unlock();
		}
	}

	public int getCachedInputBulkDataCount() {
		return mInputBuffer.getCachedBytes();
	}

	public final int readCachedBulkData(byte[] bytes, int offset) {
		try {
			mCriticalSectionLock.lock();
			int available = mInputBuffer.getCachedBytes();
			available = Math.min(available, bytes.length - offset);
			mInputBuffer.consume(available, bytes, offset);
			return available;
		} finally {
			mCriticalSectionLock.unlock();
		}
	}

	private void innerCloseOperations() {
		SocketChannel socketChannel = mChannel;
		SelectorHandler selectorHandler = mSelectorHandler;
		if (socketChannel != null) {
			if (selectorHandler != null) {
				selectorHandler.unregisterSelectionKeyProcessor(socketChannel);
			}
			if (socketChannel.isOpen()) {
				try {
					socketChannel.close();
				} catch (Exception e) {
					Logger.printException(e);
				}
			}
		}
		mOutputBuffer.skip(mOutputBuffer.getCachedBytes());
		mInputBuffer.skip(mInputBuffer.getCachedBytes());
		mChannel = null;
		mSelectorHandler = null;
	}

	private int receive(SelectionKey key) throws IOException {
		SocketChannel sc = mChannel;
		mIOBuffer.clear();
		int flag = sc.read(mIOBuffer);
		if (flag < 0) {
			throw new EOFException();
		}
		mIOBuffer.flip();
		if (!mIOBuffer.hasRemaining()) {
			return 0;
		}
		int datagramSize = mIOBuffer.limit();
		mIOBuffer.rewind();
		mIOBuffer.get(mBackBuffer, 0, datagramSize);
		mInputBuffer.offer(mBackBuffer, 0, datagramSize);
		return datagramSize;
	}

	private boolean send(byte[] datagramPacket, int offset, int length) throws IOException {
		if (datagramPacket == null) {
			return false;
		}
		mIOBuffer.clear();
		mIOBuffer.put(datagramPacket, offset, length);
		mIOBuffer.flip();
		mChannel.write(mIOBuffer);
		mIOBuffer.clear();
		return true;
	}

	private void send(AbsParserBuffer buffer) throws IOException {
		int sendLength;
		while ((sendLength = Math.min(mBackBuffer.length, buffer.getCachedBytes())) > 0) {
			buffer.consume(sendLength, mBackBuffer, 0);
			send(mBackBuffer, 0, sendLength);
		}
	}

	private AbsParserBuffer getOutputBuffer(){
		return mOutputBuffer;
	}


	protected void beforeHandleIO() {

	}

	protected void afterHandleIO() {

	}

}
