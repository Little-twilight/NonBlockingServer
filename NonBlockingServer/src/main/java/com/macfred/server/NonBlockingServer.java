package com.macfred.server;

import com.macfred.jobschedule.JobSchedule;
import com.macfred.jobschedule.JobScheduler;
import com.macfred.util.Logger;
import com.macfred.util.utils.Request;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.Selector;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NonBlockingServer implements Request.TaskScheduler {
    private SelectorHandler mSelectorHandler;
    private JobSchedulerImpl mServerJobScheduler;
    private JobScheduler mUiJobScheduler;
    private InetAddress mLocalAddress;
    private PortAllocator mPortAllocator;
    private static final String TAG = NonBlockingServer.class.getSimpleName();
    private Set<Plugin> mPlugins = new HashSet<>();

    private Lock mLock = new ReentrantLock();
    private Condition mCondition = mLock.newCondition();

    private AtomicBoolean mServerOpened = new AtomicBoolean();
    private boolean mServerRunning;
    private Thread mServerHostThread;


    public void asyncServer(InetAddress localAddress) {
        if (!mServerOpened.compareAndSet(false, true)) {
            throw new RuntimeException("Trying to start an running server");
        }
        mLocalAddress = localAddress;
        Logger.i(TAG, "Server local address assigned:" + mLocalAddress.toString());
        new Thread(() -> {
            try {
                innerServerExecution();
            } catch (IOException | InterruptedException e) {
                Logger.printException(e);
            }
        }, "Server Loop Thread").start();
    }

    public void syncServer(InetAddress localAddress) throws IOException, InterruptedException {
        if (!mServerOpened.compareAndSet(false, true)) {
            throw new RuntimeException("Trying to start an running server");
        }
        mLocalAddress = localAddress;
        Logger.i(TAG, "Server local address assigned:" + mLocalAddress.toString());
        innerServerExecution();
    }

    private void innerServerExecution() throws IOException, InterruptedException {
        try {
            mServerHostThread = Thread.currentThread();
            mSelectorHandler = new SelectorHandler(Selector.open());
            mServerJobScheduler = new JobSchedulerImpl();
            mPortAllocator = new PortAllocator(10000, 65535);
            mSelectorHandler.asyncRunner();
            mSelectorHandler.waitForStart();
            for (Plugin plugin : mPlugins) {
                if (!plugin.isStarted()) {
                    try {
                        plugin.onStart();
                    } catch (Exception e) {
                        Logger.printException(e);
                    }
                }
            }
            mServerRunning = true;

            mLock.lock();
            mCondition.signalAll();
            mLock.unlock();

            Logger.i(TAG, "Server started");

            while (mServerOpened.get()) {
                try {
                    mServerJobScheduler.checkJobSchedule();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        } finally {
            mServerRunning = false;
            for (Plugin plugin : mPlugins) {
                if (plugin.isStarted()) {
                    try {
                        plugin.onStop();
                    } catch (Exception e) {
                        Logger.printException(e);
                    }
                }
            }
            if (mSelectorHandler != null) {
                mSelectorHandler.stopAndWait();
            }
            mPortAllocator = null;
            mServerJobScheduler = null;
            mServerHostThread = null;
            mServerOpened.set(false);
            mLock.lock();
            mCondition.signalAll();
            mLock.unlock();
        }
    }

    public void installPlugin(Plugin plugin) {
        if (!mServerOpened.get()) {
            if (mPlugins.contains(plugin)) {
                return;
            }
            mPlugins.add(plugin);
            plugin.onInstall(this);
        } else {
            getJobScheduler().requestJobSchedule(new JobSchedule(() -> {
                if (mPlugins.contains(plugin)) {
                    return;
                }
                mPlugins.add(plugin);
                plugin.onInstall(this);
                if (mServerRunning) {
                    plugin.onStart();
                }
            }));
        }
    }

    public void uninstallPlugin(Plugin plugin) {
        if (!mServerOpened.get()) {
            if (mPlugins.contains(plugin)) {
                if (plugin.isStarted()) {
                    plugin.onStop();
                }
                try {
                    plugin.onUninstall();
                } finally {
                    mPlugins.remove(plugin);
                }
            }
        } else {
            getJobScheduler().requestJobSchedule(new JobSchedule(() -> {
                if (mPlugins.contains(plugin)) {
                    if (plugin.isStarted()) {
                        plugin.onStop();
                    }
                    try {
                        plugin.onUninstall();
                    } finally {
                        mPlugins.remove(plugin);
                    }
                }
            }));
        }
    }

    public void waitForStart() throws InterruptedException {
        if (!mServerOpened.get()) {
            throw new RuntimeException("Server is not opened!");
        }
        try {
            mLock.lock();
            while (!mServerRunning) {
                mCondition.await(100L, TimeUnit.MILLISECONDS);
            }
        } finally {
            mLock.unlock();
        }
    }

    public void waitForStop() throws InterruptedException {
        try {
            mLock.lock();
            while (mServerRunning) {
                mCondition.await(100L, TimeUnit.MILLISECONDS);
            }
        } finally {
            mLock.unlock();
        }
    }

    public void stop() {
        if (!mServerOpened.get()) {
            return;
        }
        if (mServerOpened.compareAndSet(true, false)) {
            Thread hostThread = mServerHostThread;
            if (hostThread != null) {
                hostThread.interrupt();
            }
        }
    }

    public void stopAndWait() throws InterruptedException {
        if (!mServerOpened.get()) {
            return;
        }
        if (mServerOpened.compareAndSet(true, false)) {
            Thread hostThread = mServerHostThread;
            if (hostThread != null) {
                hostThread.interrupt();
            }
            waitForStart();
        }

    }

    public boolean isServerOpened() {
        return mServerOpened.get();
    }

    public JobScheduler getJobScheduler() {
        return mServerJobScheduler;
    }

    public SelectorHandler getSelectorHandler() {
        return mSelectorHandler;
    }

    public InetAddress getLocalAddress() {
        return mLocalAddress;
    }

    public void updateLocalAddress(InetAddress address) {
        mLocalAddress = address;
        for (Plugin plugin : mPlugins) {
            plugin.onLocalAddressUpdated(address);
        }
    }

    public void setUiJobScheduler(JobScheduler jobScheduler) {
        mUiJobScheduler = jobScheduler;
    }

    public JobScheduler getUiJobScheduler() {
        return mUiJobScheduler;
    }

    public PortAllocator getPortAllocator() {
        return mPortAllocator;
    }

    @Override
    public Request.Task scheduleTask(Runnable action, long time) {
        JobSchedule jobSchedule = new JobSchedule(
                action,
                time
        );
        Request.Task task = new Request.Task() {
            private JobSchedule mJobSchedule = jobSchedule;
            private boolean mIsRemoved;

            @Override
            public boolean cancel() {
                if (mIsRemoved) {
                    return false;
                }
                jobSchedule.cancel();
                mIsRemoved = true;
                return true;
            }
        };
        getJobScheduler().requestJobSchedule(jobSchedule);
        return task;
    }
}

