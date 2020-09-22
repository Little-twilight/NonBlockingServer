package com.nonblockingserver;

import com.github.naturs.logger.adapter.DefaultLogAdapter;
import com.github.naturs.logger.strategy.format.PrettyFormatStrategy;
import com.nonblockingserver.util.InternetUtil;
import com.zhongyou.jobschedule.JobSchedule;
import com.zhongyou.jobschedule.JobScheduler;
import com.zhongyou.util.ZyLogger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class SocketChannelDaemon {
	private static final String TAG = SocketChannelDaemon.class.getSimpleName();
	private final SocketChannelOperator mSocketChannelOperator;
	private final SelectorHandler mSelectorHandler;
	private final JobScheduler mJobScheduler;
	private volatile InetSocketAddress mAddress;
	private volatile boolean mIsEnabled;
	private volatile int mReconnectTime;
	private volatile int mConnectTrailRemains;
	private volatile long mTimeoutMilliseconds;
	private final Object mLock = new Object();

	private JobSchedule mTimeoutCheckJobSchedule;
	private int mTimeoutCheckCount;
	private long mLatestTimeoutCheckTime;
	private final long mTimeoutCheckThermal = 1000L;

	private String uniformLogFormat(String content) {
		return String.format("(Tcp daemon addr %s) %s", mAddress, content);
	}

	public SocketChannelDaemon(SocketChannelOperator socketChannelOperator, SelectorHandler selectorHandler, JobScheduler jobScheduler) {
		mSocketChannelOperator = socketChannelOperator;
		mSelectorHandler = selectorHandler;
		mJobScheduler = jobScheduler;
		mSocketChannelOperator.getListenerManager().registerListener(update -> {
			if (mJobScheduler != null) {
				mJobScheduler.requestJobSchedule(new JobSchedule(this::checkSocketStatus));
			}
		});
	}

	/**
	 * @param address             target address
	 * @param timeoutMilliseconds 超时毫秒数
	 * @param reconnect           重连次数，0为不重连，负数未无限循环重连；
	 */

	public void enable(InetSocketAddress address, long timeoutMilliseconds, int reconnect) {
		synchronized (mLock) {
			if (mIsEnabled) {
				return;
			}
			Objects.requireNonNull(address);
			mAddress = address;
			mTimeoutMilliseconds = timeoutMilliseconds;
			mReconnectTime = reconnect;
			if (reconnect >= 0) {
				mConnectTrailRemains = reconnect + 1;
			} else {
				mConnectTrailRemains = -1;
			}
			mIsEnabled = true;
			ZyLogger.i(TAG, uniformLogFormat("Socket daemon enabled: %s"));
			mLatestTimeoutCheckTime = 0;
			mTimeoutCheckCount = 0;
			checkSocketStatus();
		}
	}

	public void disable() {
		synchronized (mLock) {
			if (!mIsEnabled) {
				return;
			}
			if (mTimeoutCheckJobSchedule != null) {
				mTimeoutCheckJobSchedule.cancel();
				mTimeoutCheckJobSchedule = null;
			}
			try {
				SocketChannelOperator.Status socketStatus = mSocketChannelOperator.getStatus();
				switch (socketStatus) {
					case Idle:
					case Disconnected:
					case PrepareDisconnection:
						return;
					default:
						mSocketChannelOperator.close();
						break;
				}
			} finally {
				ZyLogger.i(TAG, uniformLogFormat("Socket daemon disabled"));
				mIsEnabled = false;
			}
		}
	}

	private void checkSocketStatus() {
		if (!mIsEnabled) {
			return;
		}
		synchronized (mLock) {
			if (!mIsEnabled) {
				return;
			}
			if (mTimeoutCheckJobSchedule != null) {
				mTimeoutCheckJobSchedule.cancel();
				mTimeoutCheckJobSchedule = null;
			}

			long now = System.currentTimeMillis();
			long dif = now - mLatestTimeoutCheckTime;

			SocketChannelOperator.Status socketStatus = mSocketChannelOperator.getStatus();
			switch (socketStatus) {
				case Idle:
				case Disconnected:
					if (dif < mTimeoutCheckThermal) {
						mTimeoutCheckJobSchedule = new JobSchedule(this::checkSocketStatus, now + mTimeoutCheckThermal);
						mJobScheduler.requestJobSchedule(mTimeoutCheckJobSchedule);
						return;
					}
					mLatestTimeoutCheckTime = now;
					break;
				default:
					mTimeoutCheckCount = 0;
					mLatestTimeoutCheckTime = now;
					return;
			}
			String timeoutTag = mTimeoutCheckCount++ == 0 ? "first timeout check" : String.format("%s elapsed since last check", dif);
			if (mConnectTrailRemains > 0) {
				--mConnectTrailRemains;
				String majorMsg = String.format("Connection trail remains: %s", mConnectTrailRemains);
				ZyLogger.i(TAG, uniformLogFormat(String.format("%s, %s", majorMsg, timeoutTag)));
			} else if (mConnectTrailRemains == 0) {
				String majorMsg = "Disable socket daemon due to run out of connection trial";
				ZyLogger.i(TAG, uniformLogFormat(String.format("%s, %s", majorMsg, timeoutTag)));
				disable();
				return;
			} else {
				//endless reconnect
				String majorMsg = "Reconnection unlimited, keep trying";
				ZyLogger.i(TAG, uniformLogFormat(String.format("%s, %s", majorMsg, timeoutTag)));
			}

			mTimeoutCheckJobSchedule = new JobSchedule(this::checkSocketStatus, System.currentTimeMillis() + mTimeoutMilliseconds);
			mJobScheduler.requestJobSchedule(mTimeoutCheckJobSchedule);
			try {
				mSocketChannelOperator.open(mSelectorHandler, mAddress);
			} catch (IOException e) {
				ZyLogger.printException(TAG, e);
			}

		}
	}

	public long getTimeoutMilliseconds() {
		return mTimeoutMilliseconds;
	}

	public int getReconnectTime() {
		return mReconnectTime;
	}

	public InetSocketAddress getAddress() {
		return mAddress;
	}

	public boolean isEnabled() {
		return mIsEnabled;
	}


	public static void main(String... args) throws UnknownHostException, InterruptedException {
		ZyLogger.addLogAdapter(new DefaultLogAdapter(PrettyFormatStrategy.newBuilder().build()));
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
						ZyLogger.d(toString);
						byte[] data = ("Send:" + toString).getBytes();
						sendBulkData(data, 0, data.length);
					} catch (Exception e) {
						ZyLogger.printException(e);
					}
				}
			}
		};
		SocketChannelDaemon daemon = new SocketChannelDaemon(operator, server.getSelectorHandler(), server.getJobScheduler());
		daemon.enable(
				new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 12345),
				1000L,
				-1
		);
		for (int i = 0; ; i++) {
			Thread.sleep(1000L);
			String content = String.format("Hello world: %s", i);
//			ZyLogger.i(TAG, content);
			byte[] bytes = content.getBytes();
			try {
				operator.sendBulkData(bytes, 0, bytes.length);
			} catch (Exception e) {
				ZyLogger.printException(TAG, e);
			}
		}
	}

}
