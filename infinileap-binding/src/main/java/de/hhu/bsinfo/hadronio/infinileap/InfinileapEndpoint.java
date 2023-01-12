package de.hhu.bsinfo.hadronio.infinileap;

import static org.openucx.Communication.ucp_request_check_status;
import static org.openucx.Communication.ucp_request_free;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.primitive.NativeLong;
import java.io.IOException;
import java.lang.foreign.MemorySession;
import java.net.InetSocketAddress;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InfinileapEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapEndpoint.class);

    private Endpoint endpoint;
    private final InfinileapWorker worker;
    private InetSocketAddress remoteAddress;

    private final EndpointParameters parameters = new EndpointParameters();
    private final RequestParameters sendParameters = new RequestParameters().disableImmediateCompletion();
    private final RequestParameters tagReceiveParameters = new RequestParameters().disableImmediateCompletion();
    private final RequestParameters streamReceiveParameters = new RequestParameters().disableImmediateCompletion();
    private final RequestParameters emptyParameters = new RequestParameters().disableImmediateCompletion();

    private boolean errorState = false;

    InfinileapEndpoint(final Context context) throws ControlException {
        worker = new InfinileapWorker(context, new WorkerParameters());
    }

    InfinileapEndpoint(final Context context, final ConnectionRequest connectionRequest) throws ControlException {
        remoteAddress = connectionRequest.getClientAddress();
        worker = new InfinileapWorker(context, new WorkerParameters());
        endpoint = worker.getWorker().createEndpoint(
                parameters.setConnectionRequest(connectionRequest)
                .setErrorHandler((userData, endpoint, status) -> {
                    LOGGER.error("A UCX error occurred (Status: [{}])!", status);
                    handleError();
        }));

        LOGGER.info("Endpoint created");
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress) throws IOException {
        try {
            this.remoteAddress = remoteAddress;
            endpoint = worker.getWorker().createEndpoint(
                    parameters.setRemoteAddress(remoteAddress)
                    .setErrorHandler((userData, endpoint, status) -> {
                        LOGGER.error("A UCX error occurred (Status: [{}])!", status);
                        handleError();
                    }));
        } catch (ControlException e) {
            throw new IOException(e);
        }

        LOGGER.info("Endpoint created");
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var status = endpoint.sendTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), Tag.of(tag), useCallback ? sendParameters : emptyParameters);
        return checkStatus(status, blocking);
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final long tagMask, final boolean useCallback, final boolean blocking) {
        final var status = worker.getWorker().receiveTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), Tag.of(tag), Tag.of(tagMask), useCallback ? tagReceiveParameters : emptyParameters);
        return checkStatus(status, blocking);
    }

    @Override
    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var status = endpoint.sendStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), size, useCallback ? sendParameters : emptyParameters);
        checkStatus(status, blocking);
    }

    @Override
    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var receiveSize = new NativeLong();
        final var status = endpoint.receiveStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), size, receiveSize, useCallback ? streamReceiveParameters : emptyParameters);
        checkStatus(status, blocking);
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        sendParameters.setSendCallback(
            (request, status, data) -> {
                if (status == Status.OK) {
                    LOGGER.debug("Infinileap SendCallback called (Status: [{}])", status);
                    sendCallback.onMessageSent();
                } else {
                    LOGGER.error("Failed to send a message (Status: [{}])!", status);
                    handleError();
                }
            });
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        tagReceiveParameters.setReceiveCallback(
            (request, status, tagInfo, data) -> {
                if (status == Status.OK) {
                    final var tag = tagInfo.get(OfLong.JAVA_LONG, 0);
                    final var size = tagInfo.get(OfLong.JAVA_LONG, Long.BYTES);
                    LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}], Tag: [0x{}])", status, size, Long.toHexString(tag));
                    receiveCallback.onMessageReceived(tag);
                } else {
                    LOGGER.error("Failed to receive a message (Status: [{}])!", status);
                    handleError();
                }
            });

        streamReceiveParameters.setReceiveCallback(
                (request, status, length, data) -> {
                    if (status == Status.OK) {
                        LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}])", status, length);
                        receiveCallback.onMessageReceived(0);
                    } else {
                        LOGGER.error("Failed to receive a message (Status: [{}])!", status);
                        handleError();
                    }
                });
    }

    @Override
    public boolean getErrorState() {
        return errorState;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
        endpoint.close();
    }

    private void handleError() {
        errorState = true;
    }

    private boolean checkStatus(long status, boolean blocking) {
        if (Status.isStatus(status)) {
            return Status.is(status, Status.OK);
        }

        long statusCode = ucp_request_check_status(status);
        while (blocking && Status.is(statusCode, Status.IN_PROGRESS)) {
            worker.getWorker().progress();
            statusCode = ucp_request_check_status(status);
        }

        return Status.is(statusCode, Status.OK);
    }
}
