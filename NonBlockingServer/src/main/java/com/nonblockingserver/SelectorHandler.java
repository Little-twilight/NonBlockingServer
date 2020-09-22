package com.nonblockingserver;

import com.zhongyou.util.ZyLogger;
import com.zhongyou.util.function.Consumer;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class SelectorHandler {
	private static final String TAG = SelectorHandler.class.getSimpleName();
	private Map<Channel, Consumer<SelectionKey>> mSelectionKeyProcessors = new HashMap<>();
	private Lock mLock = new ReentrantLock();
	private Condition mCondition = mLock.newCondition();
	private AtomicBoolean mIsOpened = new AtomicBoolean();
	private boolean mIsRunning;
	private Thread mHostThread;
	private SelectorProxy mSelectorProxy;

	private Consumer<Iterator<SelectionKey>> mSelectionKeyHandler = iterator -> {
		while (iterator.hasNext()) {
			SelectionKey key = null;
			try {
				key = iterator.next();
				iterator.remove();
				Channel channel = key.channel();
				Consumer<SelectionKey> processor = mSelectionKeyProcessors.get(channel);
				if (processor != null) {
					processor.accept(key);
				}
			} catch (Exception e) {
				ZyLogger.printException(e);
				Throwable cause = e.getCause();
				if (cause instanceof SocketException) {
					String message = cause.getMessage();
					if ("Network is unreachable".equals(message)
							|| "Operation not permitted".equals(message)) {
						return;//do not close socket here
					}
					return;//全都放过
				}

				ZyLogger.e(TAG, "Channel closed due to exception");
				ZyLogger.printException(TAG, e);
				try {
					key.cancel();
					key.channel().close();
				} catch (IOException cex) {
					ZyLogger.printException(e);
				}
				if (e instanceof InterruptedException) {
					Thread.interrupted();
					return;
				}
			}
		}
	};

	SelectorHandler(Selector selector) {
		mSelectorProxy = new SelectorProxy(selector);
	}

	void asyncRunner() {
		if (!mIsOpened.compareAndSet(false, true)) {
			throw new RuntimeException("Trying to start an running selector handler");
		}
		new Thread(this::innerRunner, "Network IO Selector Thread").start();
	}

	void syncRunner() {
		if (!mIsOpened.compareAndSet(false, true)) {
			throw new RuntimeException("Trying to start an running selector handler");
		}
		innerRunner();
	}

	private void innerRunner() {
		try {
			mHostThread = Thread.currentThread();
			try {
				registerSelectionKeyProcessor(mSelectorProxy.open(), mSelectorProxy.getPipeHandler());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			mIsRunning = true;
			mLock.lock();
			mCondition.signalAll();
			mLock.unlock();
			while (mIsOpened.get()) {
				handleSelectorIO();
			}
		} catch (Exception e) {
			ZyLogger.printException(e);
			if (e instanceof InterruptedException) {
				Thread.interrupted();
			}
		} finally {
			mLock.lock();
			mIsRunning = false;
			mIsOpened.set(false);
			try {
				mSelectorProxy.close();
			} catch (IOException e) {
				ZyLogger.printException(e);
			}
			mSelectionKeyProcessors.clear();
			mHostThread = null;
			mCondition.signalAll();
			mLock.unlock();
		}
	}

	void waitForStart() throws InterruptedException {
		if (!mIsOpened.get()) {
			throw new RuntimeException("SelectorHandler is not opened!");
		}
		try {
			mLock.lock();
			while (!mIsRunning) {
				mCondition.await(100L, TimeUnit.MILLISECONDS);
			}
		} finally {
			mLock.unlock();
		}
	}

	boolean isRunning() {
		return mIsRunning;
	}

	void stop() throws IOException {
		try {
			mLock.lock();
			if (!mIsOpened.get()) {
				return;
			}
			mIsOpened.set(false);
			if (mIsRunning) {
				SelectorProxy selectorProxy = mSelectorProxy;
				if (selectorProxy != null) {
					selectorProxy.wakeup();
					selectorProxy.mBackupSelector.close();
				}
				Thread hostThread = mHostThread;
				if (hostThread != null) {
					hostThread.interrupt();
				}
			}
		} finally {
			mLock.unlock();
		}
	}

	void stopAndWait() throws InterruptedException, IOException {
		try {
			mLock.lock();
			if (!mIsOpened.get()) {
				return;
			}
			mIsOpened.set(false);
			if (mIsRunning) {
				SelectorProxy selectorProxy = mSelectorProxy;
				if (selectorProxy != null) {
					selectorProxy.wakeup();
					selectorProxy.mBackupSelector.close();
				}
				Thread hostThread = mHostThread;
				if (hostThread != null) {
					hostThread.interrupt();
				}
			}
			while (mIsRunning) {
				mCondition.await(200L, TimeUnit.MILLISECONDS);
			}
		} finally {
			mLock.unlock();
		}
	}


	private void handleSelectorIO() throws IOException {
		mSelectorProxy.selectAndProcess(mSelectionKeyHandler);
	}

	void registerSelectionKeyProcessor(Channel channel, Consumer<SelectionKey> processor) {
		mSelectionKeyProcessors.put(channel, processor);
	}

	void unregisterSelectionKeyProcessor(Channel channel) {
		mSelectionKeyProcessors.remove(channel);
	}

	SelectorProxy getSelectorProxy() {
		return mSelectorProxy;
	}

	class SelectorProxy {
		private Selector mBackupSelector;
		private final int mBufferSize = 400;
		private Queue<Runnable> mTasks = new ConcurrentLinkedQueue<>();

		private ByteBuffer mIOBufferSource;
		private byte[] mBackBufferSource;
		private ByteBuffer mIOBufferSink;
		private byte[] mBackBufferSink;
		private Pipe mPipe;
		private Consumer<SelectionKey> mPipeHandler = new Consumer<SelectionKey>() {
			@Override
			public void accept(SelectionKey selectionKey) {
				if (selectionKey.isReadable()) {
					mIOBufferSource.clear();
					try {
						mPipe.source().read(mIOBufferSource);
					} catch (IOException e) {
						ZyLogger.printException(e);
					}
					mIOBufferSource.flip();
					if (!mIOBufferSource.hasRemaining()) {
						return;
					}
					int dataSize = mIOBufferSource.limit();
					mIOBufferSource.rewind();
					mIOBufferSource.get(mBackBufferSource, 0, dataSize);
				}
				Runnable task;
				while ((task = mTasks.poll()) != null) {
					task.run();
				}
			}
		};

		SelectorProxy(Selector selector) {
			mBackupSelector = selector;
			mIOBufferSource = ByteBuffer.allocate(mBufferSize);
			mBackBufferSource = new byte[mBufferSize];
			mIOBufferSink = ByteBuffer.allocate(mBufferSize);
			mBackBufferSink = new byte[mBufferSize];
		}

		private Pipe.SourceChannel open() throws IOException {
			mPipe = Pipe.open();
			mPipe.source().configureBlocking(false);
			addInterestOps(mPipe.source(), SelectionKey.OP_READ);
			return mPipe.source();
		}

		private void close() throws IOException {
			if (mPipe != null) {
				mPipe.source().close();
				mPipe.sink().close();
				mPipe = null;
			}
		}

		private Consumer<SelectionKey> getPipeHandler() {
			return mPipeHandler;
		}

		private void register(SelectableChannel channel, Runnable action) {
			Thread thread = mHostThread;
			if (thread != null && thread == Thread.currentThread()) {
				action.run();
				return;
			}
			long now = System.currentTimeMillis();
			try {
				mTasks.offer(() -> {
					ZyLogger.v(TAG, "register:" + (System.currentTimeMillis() - now));
					try {
						action.run();
					} catch (RuntimeException e) {
						ZyLogger.printException(e);
					}

				});
				mIOBufferSink.clear();
				mIOBufferSink.put(mBackBufferSink, 0, 1);
				mIOBufferSink.flip();
				mPipe.sink().write(mIOBufferSink);
				mIOBufferSink.clear();
			} catch (IOException e) {
				ZyLogger.printException(e);
			}
		}

		void selectAndProcess(Consumer<Iterator<SelectionKey>> selectionConsumer) throws IOException {
			if (mBackupSelector.select() > 0) {
				selectionConsumer.accept(mBackupSelector.selectedKeys().iterator());
			}
		}

		void wakeup() {
			mBackupSelector.wakeup();
		}

		int interestOps(SelectableChannel channel) {
			SelectionKey key = channel.keyFor(mBackupSelector);
			if (key == null) {
				return 0;
			}
			return key.interestOps();
		}

		void updateInterestOps(SelectableChannel channel, int opt) {
			Runnable action = () -> {
				int optionsNow = interestOps(channel);
				if (opt == optionsNow) {
					return;
				}
				try {
					channel.register(mBackupSelector, opt);
				} catch (ClosedChannelException e) {
					throw new RuntimeException(e);
				}
			};
			register(channel, action);
		}

		void addInterestOps(SelectableChannel channel, int opt) {
			Runnable action = () -> {
				int optionsNow = interestOps(channel);
				if ((optionsNow & opt) == opt) {
					return;
				}
				try {
					channel.register(mBackupSelector, optionsNow | opt);
				} catch (ClosedChannelException e) {
					throw new RuntimeException(e);
				}
			};
			register(channel, action);
		}

		void removeInterestOps(SelectableChannel channel, int opt) {
			Runnable action = () -> {
				int optionsNow = interestOps(channel);
				if ((optionsNow & opt) == 0) {
					return;
				}
				try {
					channel.register(mBackupSelector, optionsNow & (~opt));
				} catch (ClosedChannelException e) {
					throw new RuntimeException(e);
				}
			};
			register(channel, action);
		}

	}
}
