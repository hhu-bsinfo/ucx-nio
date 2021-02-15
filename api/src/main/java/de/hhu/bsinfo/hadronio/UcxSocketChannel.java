package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class UcxSocketChannel extends SocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxSocketChannel.class);
    private static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;
    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(1048576);
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(1048576);

    private UcpEndpoint endpoint;
    private UcxCallback sendCallback;
    private UcxCallback receiveCallback;

    private boolean connected = false;
    private boolean inputClosed = false;
    private boolean outputClosed = false;
    private int receivePosition = 0;
    private int readyOps = 0;

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context) {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpConnectionRequest connectionRequest) throws IOException {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());

        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        connected = establishConnection();
    }

    @Override
    public SocketChannel bind(SocketAddress socketAddress) throws IOException {
        LOGGER.warn("Trying to bind socket channel to [{}], but binding is not supported", socketAddress);

        return this;
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        LOGGER.warn("Trying to set option [{}], which is not supported", socketOption.name());

        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
        LOGGER.warn("Trying to get option [{}], which is not supported", socketOption.name());

        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be readable");

        inputClosed = true;

        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be writeable");

        outputClosed = true;

        return this;
    }

    @Override
    public Socket socket() {
        LOGGER.error("Direct socket access is not supported");

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean connect(SocketAddress remoteAddress) throws IOException {
        LOGGER.info("Connecting to [{}]", remoteAddress);

        return (connected = connectTo(remoteAddress));
    }

    private boolean connectTo(SocketAddress remoteAddress) {
        UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        return establishConnection();
    }

    private boolean establishConnection() {
        ByteBuffer sendBuffer = ByteBuffer.allocateDirect(8);
        ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(8);

        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.rewind();

        ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer);

        LOGGER.info("Exchanging small message to establish connection");

        endpoint.sendTaggedNonBlocking(sendBuffer, connectionCallback);
        worker.recvTaggedNonBlocking(receiveBuffer, connectionCallback);

        return connected;
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (connected) {
            readyOps &= ~SelectionKey.OP_CONNECT;
            return true;
        }

        if (isBlocking()) {
            LOGGER.info("Waiting for connection to be established");

            while (!connected) {
                try {
                    while (worker.progress() == 0) {
                        worker.waitForEvents();
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to progress worker while waiting for connection to be established", e);
                }
            }
        }

        readyOps &= ~SelectionKey.OP_CONNECT;

        return connected;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        LOGGER.warn("Trying to get remote address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        if (inputClosed) {
            return -1;
        }

        synchronized (receiveBuffer) {
            int numBytes = Math.min(Math.abs(receiveBuffer.position() - receivePosition), byteBuffer.remaining());

            LOGGER.debug("Reading [{}] bytes (receiveBuffer.position: [{}], receivePosition: [{}])", numBytes, receiveBuffer.position(), receivePosition);

            if (numBytes == 0) {
                return 0;
            }

            for (int i = 0; i < numBytes; i++) {
                byteBuffer.put(receiveBuffer.get((receivePosition + i) % receiveBuffer.capacity()));
            }

            receivePosition = (receivePosition + numBytes) % receiveBuffer.capacity();

            return numBytes;
        }
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        if (inputClosed) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        if (outputClosed) {
            return -1;
        }

        synchronized (sendBuffer) {
            int numBytes = Math.min(sendBuffer.remaining(), byteBuffer.remaining());

            LOGGER.debug("Writing [{}] bytes (sendBuffer.position: [{}])", numBytes, sendBuffer.position());

            if (numBytes == 0) {
                return 0;
            }

            int oldPosition = sendBuffer.position();

            for (int i = 0; i < numBytes; i++) {
                sendBuffer.put(byteBuffer.get(i));
            }

            sendBuffer.position(oldPosition);
            sendBuffer.limit(sendBuffer.position() + numBytes);

            endpoint.sendStreamNonBlocking(sendBuffer, sendCallback);

            sendBuffer.position(sendBuffer.limit() % sendBuffer.capacity());
            sendBuffer.limit(sendBuffer.capacity());

            return numBytes;
        }
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        if (outputClosed) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        LOGGER.warn("Trying to get local address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing socket channel");
        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        if (connected && !outputClosed) {
            readyOps |= SelectionKey.OP_WRITE;
        } else {
            readyOps &= ~SelectionKey.OP_WRITE;
        }

        if (receivePosition != receiveBuffer.position() && !inputClosed) {
            readyOps |= SelectionKey.OP_READ;
        } else {
            readyOps &= ~SelectionKey.OP_READ;
        }

        return readyOps;
    }

    @Override
    public void select() throws IOException {
        try {
            worker.progress();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static final class ConnectionCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

        private final UcxSocketChannel socket;
        private final ByteBuffer receiveBuffer;
        private final AtomicInteger successCounter = new AtomicInteger(0);

        private ConnectionCallback(UcxSocketChannel socket, ByteBuffer receiveBuffer) {
            this.socket = socket;
            this.receiveBuffer = receiveBuffer;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            if (request.isCompleted()) {
                int count = successCounter.incrementAndGet();

                LOGGER.info("Connection callback has been called with a successfully completed request ([{}/2])", successCounter.get());

                if (count == 2) {
                    long magic = receiveBuffer.getLong();

                    if (magic != CONNECTION_MAGIC_NUMBER) {
                        LOGGER.error("Connection callback has been called, but magic number is wrong! Expected: [{}], Received: [{}] -> Discarding connection", Long.toHexString(CONNECTION_MAGIC_NUMBER), Long.toHexString(magic));
                        return;
                    }

                    socket.sendCallback = new SendCallback(socket, socket.sendBuffer);
                    socket.receiveCallback = new ReceiveCallback(socket, socket.receiveBuffer);

                    socket.endpoint.recvStreamNonBlocking(socket.receiveBuffer, 0, socket.receiveCallback);

                    socket.connected = true;
                    socket.readyOps |= SelectionKey.OP_CONNECT;

                    successCounter.set(0);
                }
            }
        }
    }

    private static final class SendCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

        private final UcxSocketChannel socket;
        private final ByteBuffer sendBuffer;

        private SendCallback(UcxSocketChannel socket, ByteBuffer sendBuffer) {
            this.socket = socket;
            this.sendBuffer = sendBuffer;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            LOGGER.debug("SendCallback called (Completed: [{}])", request.isCompleted());
        }
    }

    private static final class ReceiveCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

        private final UcxSocketChannel socket;
        private final ByteBuffer receiveBuffer;

        private ReceiveCallback(UcxSocketChannel socket, ByteBuffer receiveBuffer) {
            this.socket = socket;
            this.receiveBuffer = receiveBuffer;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}])", request.isCompleted(), request.getRecvSize());

            synchronized (socket.receiveBuffer) {
                receiveBuffer.position((int) ((receiveBuffer.position() + request.getRecvSize()) % receiveBuffer.capacity()));
            }

            socket.endpoint.recvStreamNonBlocking(receiveBuffer, 0, this);
        }
    }
}
