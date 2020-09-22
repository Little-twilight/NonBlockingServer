package com.nonblockingserver;

import com.zhongyou.util.ZyLogger;
import com.zhongyou.util.function.Consumer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 处理Selector的操作在一个线程，其余的操作在一个单独线程，不能混用
 */
public abstract class DatagramChannelOperator {

	private final int mBufferSize;
	private final ByteBuffer mIOBuffer;
	private final byte[] mBackBuffer;
	private DatagramChannel mChannel;
	private Queue<DatagramPacket> mDatagramPacketToSend;
	private Queue<PacketBundle> mPendingReceivedPackets;
	private SelectorHandler mSelectorHandler;
	private volatile boolean mIsOpened;
	private volatile boolean mIsOpeningChannel;
	private volatile boolean mIsClosingChannel;

	private volatile Lock mIoCriticalSectionLock = new ReentrantLock();


	private volatile boolean mIsWaitingForPendingDatagramToBeSent;
	private Lock mLockWaitForPendingDatagramToBeSent = new ReentrantLock();
	private Condition mConditionWaitForPendingDatagramToBeSent = mLockWaitForPendingDatagramToBeSent.newCondition();

	private final Consumer<SelectionKey> mSelectionProcessor = key -> {
		beforeHandleIO();
		try {
			mIoCriticalSectionLock.lock();
			if (mIsOpened && !mIsClosingChannel) {
				if (key.isReadable()) {
					DatagramPacket datagramPacket;
					while ((datagramPacket = receive(key)) != null) {
						if (!onNewDatagramReceived(datagramPacket)) {
							mPendingReceivedPackets.offer(new PacketBundle(
									System.currentTimeMillis(),
									datagramPacket
							));
						}
					}
				}
				if (key.isWritable() && mIsOpened && !mIsClosingChannel) {
					DatagramPacket datagramPacket;
					boolean signal = mIsWaitingForPendingDatagramToBeSent;
					try {
						if (signal) {
							mLockWaitForPendingDatagramToBeSent.lock();
						}
						while ((datagramPacket = mDatagramPacketToSend.poll()) != null) {
							send(datagramPacket);
						}
						mSelectorHandler.getSelectorProxy().removeInterestOps(mChannel, SelectionKey.OP_WRITE);
					} finally {
						if (signal) {
							mConditionWaitForPendingDatagramToBeSent.signalAll();
							mLockWaitForPendingDatagramToBeSent.unlock();
						}
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			mIoCriticalSectionLock.unlock();
		}

		afterHandleIO();
	};


	public DatagramChannelOperator(int bufferSize) {
		mBufferSize = bufferSize;
		mIOBuffer = ByteBuffer.allocate(mBufferSize);
		mBackBuffer = new byte[mBufferSize];
		mDatagramPacketToSend = new ConcurrentLinkedQueue<>();
		mPendingReceivedPackets = new ArrayDeque<>();
	}

	private Consumer<SelectionKey> getSelectionKeyProcessor() {
		return mSelectionProcessor;
	}

	public DatagramChannel open(SelectorHandler selectorHandler, int listenPort) throws IOException {
		if (selectorHandler == null) {
			throw new NullPointerException("Null SelectorHandler specification ");
		}
		try {
			mIoCriticalSectionLock.lock();
			mIsOpeningChannel = true;
			if (mIsOpened) {
				throw new RuntimeException("Already opened ");
			}
			mChannel = DatagramChannel.open();
			mChannel.configureBlocking(false);
			mChannel.socket().bind(new InetSocketAddress(listenPort));
			mSelectorHandler = selectorHandler;
			mSelectorHandler.getSelectorProxy().addInterestOps(mChannel, SelectionKey.OP_READ);
			mSelectorHandler.registerSelectionKeyProcessor(getChannel(), getSelectionKeyProcessor());
			mIsOpened = true;
			return mChannel;
		} catch (Exception e) {
			close();
			throw e;
		} finally {
			mIsOpeningChannel = false;
			mIoCriticalSectionLock.unlock();
		}
	}

	public void close() {
		try {
			mIoCriticalSectionLock.lock();
			mIsClosingChannel = true;
			if (!mIsOpened) {
				return;
			}
			mIsOpened = false;
			mDatagramPacketToSend.clear();
			mPendingReceivedPackets.clear();
			mSelectorHandler.unregisterSelectionKeyProcessor(getChannel());
			if (mChannel != null && mChannel.isOpen()) {
				try {
					mChannel.close();
				} catch (Exception e) {
					ZyLogger.printException(e);
				}
				mChannel = null;
			}
			mSelectorHandler = null;
		} finally {
			mIsClosingChannel = false;
			mIoCriticalSectionLock.unlock();
		}
	}

	public void waitForPendingDatagramToBeSent(long timeout) {
		if (!mIsOpened) {
			return;
		}
		mIsWaitingForPendingDatagramToBeSent = true;
		try {
			mLockWaitForPendingDatagramToBeSent.lock();
			if (!mDatagramPacketToSend.isEmpty()) {
				try {
					mConditionWaitForPendingDatagramToBeSent.await(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					ZyLogger.printException(e);
					Thread.interrupted();
				}
			}
		} finally {
			mLockWaitForPendingDatagramToBeSent.unlock();
			mIsWaitingForPendingDatagramToBeSent = false;
		}
	}

	public boolean isOpened() {
		return mIsOpened;
	}

	private DatagramPacket receive(SelectionKey key) throws IOException {
		if (key == null) {
			return null;
		}
		DatagramChannel sc = (DatagramChannel) key.channel();
		mIOBuffer.clear();
		InetSocketAddress address = (InetSocketAddress) sc.receive(mIOBuffer);
		mIOBuffer.flip();
		if (!mIOBuffer.hasRemaining()) {
			return null;
		}
		int datagramSize = mIOBuffer.limit();
		mIOBuffer.rewind();
		mIOBuffer.get(mBackBuffer, 0, datagramSize);
		byte[] datagram = new byte[datagramSize];
		System.arraycopy(mBackBuffer, 0, datagram, 0, datagramSize);
		DatagramPacket packet = new DatagramPacket(datagram, datagramSize);
		packet.setSocketAddress(address);
		return packet;
	}

	protected void beforeHandleIO() {

	}

	protected void afterHandleIO() {

	}

	private boolean send(DatagramPacket datagramPacket) throws IOException {
		if (datagramPacket == null) {
			return false;
		}
		if (!mIsOpened || mIsClosingChannel) {
			throw new RuntimeException("Try to send datagram while closed");
		}
		mIOBuffer.clear();
		mIOBuffer.put(datagramPacket.getData(), datagramPacket.getOffset(), datagramPacket.getLength());
		mIOBuffer.flip();
		InetSocketAddress target = new InetSocketAddress(datagramPacket.getAddress(), datagramPacket.getPort());
		mChannel.send(mIOBuffer, target);
		mIOBuffer.clear();
		return true;
	}

	protected boolean onNewDatagramReceived(DatagramPacket datagramPacket) {
		return true;
	}

	public boolean checkPendingReceivedPacket() {
		return !mPendingReceivedPackets.isEmpty();
	}

	public void setBroadcast(boolean broadcast) throws SocketException {
		mChannel.socket().setBroadcast(broadcast);
	}

	public PacketBundle nextPendingReceivedPacket() {
		return mPendingReceivedPackets.poll();
	}

	public final void sendDatagram(DatagramPacket datagramPacket) {
		if (!mIsOpened) {
			throw new RuntimeException("Send datagram while closed");
		}
		mDatagramPacketToSend.offer(datagramPacket);
		mSelectorHandler.getSelectorProxy().addInterestOps(mChannel, SelectionKey.OP_WRITE);

	}

	private DatagramChannel getChannel() {
		return mChannel;
	}

	public class PacketBundle {
		public long time;
		public DatagramPacket packet;

		PacketBundle(long time, DatagramPacket packet) {
			this.time = time;
			this.packet = packet;
		}
	}
}
