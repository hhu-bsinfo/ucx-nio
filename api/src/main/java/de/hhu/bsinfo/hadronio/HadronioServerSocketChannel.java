package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;

public class HadronioServerSocketChannel extends ServerSocketChannel implements HadronioSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioServerSocketChannel.class);
    private static final int DEFAULT_SERVER_PORT = 2998;
    private final UcxServerSocketChannel serverSocketChannel;

    private InetSocketAddress localAddress;

    private boolean channelClosed = false;

    public HadronioServerSocketChannel(final SelectorProvider provider, final UcxServerSocketChannel serverSocketChannel) {
        super(provider);
        serverSocketChannel.configureBlocking(isBlocking());

        this.serverSocketChannel = serverSocketChannel;
    }

    @Override
    public ServerSocketChannel bind(final SocketAddress socketAddress, final int backlog) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (localAddress != null) {
            throw new AlreadyBoundException();
        }

        if (socketAddress == null) {
            localAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);
        } else if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        } else {
            localAddress = (InetSocketAddress) socketAddress;
        }

        serverSocketChannel.bind(localAddress, backlog);
        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(final SocketOption<T> socketOption, T t) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public <T> T getOption(final SocketOption<T> socketOption) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public ServerSocket socket() {
        throw new UnsupportedOperationException("Direct socket access is not supported!");
    }

    @Override
    public SocketChannel accept() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (localAddress == null) {
            throw new NotYetBoundException();
        }

        return serverSocketChannel.accept();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return localAddress;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing server socket channel bound to [{}]", localAddress);
        channelClosed = true;
        serverSocketChannel.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        serverSocketChannel.configureBlocking(blocking);
        LOGGER.info("Server socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        return serverSocketChannel.readyOps();
    }

    @Override
    public void select() throws IOException {
        serverSocketChannel.select();
    }
}
