package com.macfred.server;

import com.github.naturs.logger.adapter.DefaultLogAdapter;
import com.github.naturs.logger.strategy.format.PrettyFormatStrategy;
import com.macfred.jobschedule.JobSchedule;
import com.macfred.protocol.parser.AbsParserBuffer;
import com.macfred.server.util.InternetUtil;
import com.macfred.util.Logger;
import com.macfred.util.function.Consumer;
import com.macfred.util.utils.ListenerManager;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
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

    private volatile Status mStatus = Status.Idle;
    private final ListenerManager<StatusListener> mListenerManager = new ListenerManager<>();

    private Lock mCriticalSectionLock = new ReentrantLock();
    private Condition mCriticalSectionCondition = mCriticalSectionLock.newCondition();
    private InetSocketAddress mDstAddress;

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
                        Logger.printException(TAG, e);
                    }
                }
            }
        };
        operator.open(server.getSelectorHandler(), new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 12345));
    }

    public ListenerManager<StatusListener> getListenerManager() {
        return mListenerManager;
    }

    public enum Status {
        Idle,
        ConnectionRefused,
        PrepareConnection,
        ConnectionPending,
        HandlePendingConnection,
        Established,
        HandleEstablishedConnection,
        PrepareDisconnection,
        Disconnected
    }

    public Status getStatus() {
        return mStatus;
    }

    public interface StatusListener {
        void onStatusUpdate(Status update);
    }

    private void update(Status update) {
        mCriticalSectionLock.lock();
        try {
            if (mStatus == update) {
                return;
            }
            mStatus = update;
        } finally {
            mCriticalSectionLock.unlock();
        }
        mListenerManager.forEachListener(statusListener -> statusListener.onStatusUpdate(update));
    }

    private String uniformLogFormat(String content) {
        return String.format("(Tcp addr %s, status %s) %s", mDstAddress, mStatus, content);
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
                            mSelectorHandler.getSelectorProxy().addInterestOps(mChannel, SelectionKey.OP_READ);
                            update(Status.Established);
                        } catch (ConnectException e) {
                            Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("Connection exception when handle pending tcp: %s ", e.getMessage())), e));
                            update(Status.ConnectionRefused);
                            //broken socket, just close
                            Logger.i(TAG, uniformLogFormat("About to close pending tcp due to exception"));
                            close();
                        } catch (IOException e) {
                            Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("IO exception occurred when handle pending tcp: %s ", e.getMessage())), e));
                            update(Status.ConnectionPending);
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
                            Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("EOF occurred when read from tcp: %s ", e.getMessage())), e));
                            update(Status.PrepareDisconnection);
                            innerCloseOperations();
                            Logger.i(TAG, uniformLogFormat("Tcp socket closed due EOF when read from tcp"));
                            update(Status.Disconnected);
                            break;
                        } catch (IOException e) {
                            Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("IO exception occurred when read from tcp: %s ", e.getMessage())), e));
                        }
                    }
                    if (key.isWritable()) {
                        try {
                            AbsParserBuffer outputBuffer = getOutputBuffer();
                            if (outputBuffer.getCachedBytes() > 0) {
                                send(outputBuffer);
                                mCriticalSectionCondition.signalAll();
                            }
                            mSelectorHandler.getSelectorProxy().removeInterestOps(mChannel, SelectionKey.OP_WRITE);
                        } catch (IOException e) {
                            Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("IO exception occurred when write to tcp: %s ", e.getMessage())), e));
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
                    Logger.w(TAG, String.format("Open %s on illegal status %s", SocketChannelOperator.class.getSimpleName(), status));
                    return false;
            }
            update(Status.PrepareConnection);
            SocketChannel socketChannel = null;
            try {
                mDstAddress = dstAddress;
                socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(false);
                socketChannel.connect(dstAddress);
            } catch (Exception e) {
                Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("Exception occurred when open tcp socket: %s ", e.getMessage())), e));
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
                selectorHandler.getSelectorProxy().addInterestOps(socketChannel, SelectionKey.OP_CONNECT);
            } catch (Exception e) {
                Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("IO exception occurred when register tcp selector: %s ", e.getMessage())), e));
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
                case ConnectionRefused:
                    break;
                default:
                    return false;
            }
            update(Status.PrepareDisconnection);
            innerCloseOperations();
            Logger.i(uniformLogFormat("TCP Socket closed"));
            update(Status.Disconnected);
            return true;
        } finally {
            mCriticalSectionLock.unlock();
        }
    }

    public final boolean sendBulkData(byte[] bulkData, int offset, int length) {
        mCriticalSectionLock.lock();
        try {
            Status status = getStatus();
            switch (status) {
                case ConnectionPending:
                case PrepareConnection:
                case HandlePendingConnection:
                case HandleEstablishedConnection:
                case Established:
                    break;
                default:
                    return false;
            }
            mOutputBuffer.offer(bulkData, offset, length);
            mSelectorHandler.getSelectorProxy().addInterestOps(mChannel, SelectionKey.OP_WRITE);
            return true;
        } finally {
            mCriticalSectionLock.unlock();
        }
    }

    public final void waitForOutputDataToBeSent(long timeoutMilliseconds) throws TimeoutException, InterruptedException {
        long initialTime = System.currentTimeMillis();
        long timeoutBoundary = initialTime + timeoutMilliseconds;
        if (!mCriticalSectionLock.tryLock(timeoutMilliseconds, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Unable to acquire lock within timeout period");
        }
        try {
            while (true) {
                Status status = getStatus();
                switch (status) {
                    case Idle:
                    case Disconnected:
                        return;
                    case ConnectionRefused:
                    case ConnectionPending:
                    case PrepareConnection:
                    case HandlePendingConnection:
                    case HandleEstablishedConnection:
                    case Established:
                    case PrepareDisconnection:
                        break;
                    default:
                        throw new RuntimeException(String.format("Unsupported status %s when waitForOutputDataToBeSent", status));
                }
                int pendingOutputBytes = mOutputBuffer.getCachedBytes();
                if (pendingOutputBytes == 0) {
                    return;
                }
                long now = System.currentTimeMillis();
                long remainingTimeout = timeoutBoundary - now;
                if (remainingTimeout <= 0) {
                    throw new TimeoutException("Timeout elapsed");
                }
                mCriticalSectionCondition.await(remainingTimeout, TimeUnit.MILLISECONDS);
            }
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
                    Logger.printException(TAG, new RuntimeException(uniformLogFormat(String.format("Exception occurred when close tcp: %s ", e.getMessage())), e));
                }
            }
        }
        mOutputBuffer.clear();
        mInputBuffer.clear();
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

    private AbsParserBuffer getOutputBuffer() {
        return mOutputBuffer;
    }


    protected void beforeHandleIO() {

    }

    protected void afterHandleIO() {

    }

}
