/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.transport.controlmessage;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.transport.clientapi.ErrorResponseWriter;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.FragmentHandler;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.protocol.clientapi.ControlMessageRequestDecoder;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.ErrorCode;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerResponse;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.ZbActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.zeebe.broker.services.DispatcherSubscriptionNames.TRANSPORT_CONTROL_MESSAGE_HANDLER_SUBSCRIPTION;

public class ControlMessageHandlerManager extends ZbActor implements FragmentHandler
{
    public static final Logger LOG = Loggers.TRANSPORT_LOGGER;

    protected static final String NAME = "control.message.handler";

    protected final ZbActorScheduler actorScheduler;
    private ControlMessageType lastRequestMessageType;

    protected final ControlMessageRequestHeaderDescriptor requestHeaderDescriptor = new ControlMessageRequestHeaderDescriptor();
    protected final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    protected final ControlMessageRequestDecoder requestDecoder = new ControlMessageRequestDecoder();

    protected final UnsafeBuffer requestBuffer = new UnsafeBuffer(new byte[1024 * 32]);

    protected final Dispatcher controlMessageDispatcher;

    protected final Int2ObjectHashMap<ControlMessageHandler> handlersByTypeId = new Int2ObjectHashMap<>();

    protected final ErrorResponseWriter errorResponseWriter;
    protected final BrokerEventMetadata eventMetada = new BrokerEventMetadata();
    protected final ServerResponse response = new ServerResponse();

    protected final long requestTimeoutInMillis;

    public ControlMessageHandlerManager(
            ServerOutput output,
            Dispatcher controlMessageDispatcher,
            long requestTimeoutInMillis,
            ZbActorScheduler actorScheduler,
            List<ControlMessageHandler> handlers)
    {
        this.actorScheduler = actorScheduler;
        this.controlMessageDispatcher = controlMessageDispatcher;
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.errorResponseWriter = new ErrorResponseWriter(output);

        for (ControlMessageHandler handler : handlers)
        {
            final ControlMessageType messageType = handler.getMessageType();
            handlersByTypeId.put(messageType.value(), handler);
        }
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    protected void onActorStarted()
    {
        final ActorFuture<Subscription> subscriptionAsync = controlMessageDispatcher.openSubscriptionAsync(TRANSPORT_CONTROL_MESSAGE_HANDLER_SUBSCRIPTION);

        actor.runOnCompletion(subscriptionAsync, (sub, throwable) ->
        {
            if (throwable == null)
            {
                actor.consume(sub, () -> sub.poll(this, 1));
                openFuture.complete(null);
            }
            else
            {
                openFuture.completeExceptionally(throwable);
                Loggers.SYSTEM_LOGGER.error("Can't get subscription for {}", NAME, throwable);
            }
        });
    }


    private final CompletableActorFuture<Void> openFuture = new CompletableActorFuture<>();
    private final AtomicBoolean isOpenend = new AtomicBoolean(false);

    public ActorFuture<Void> openAsync()
    {
        openFuture.close();
        openFuture.setAwaitingResult();

        if (isOpenend.compareAndSet(false, true))
        {
            actorScheduler.submitActor(this);
        }
        else
        {
            openFuture.complete(null);
        }

        return openFuture;
    }

    public ActorFuture<Void> closeAsync()
    {
        if (isOpenend.compareAndSet(true, false))
        {
            return actor.close();
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    @Override
    public int onFragment(DirectBuffer buffer, int offset, int length, int streamId, boolean isMarkedFailed)
    {
        requestHeaderDescriptor.wrap(buffer, offset);

        eventMetada.reset();

        eventMetada
            .requestId(requestHeaderDescriptor.requestId())
            .requestStreamId(requestHeaderDescriptor.streamId());

        offset += ControlMessageRequestHeaderDescriptor.headerLength();

        messageHeaderDecoder.wrap(requestBuffer, 0);
        offset += messageHeaderDecoder.encodedLength();

        requestDecoder.wrap(buffer, offset, requestDecoder.sbeBlockLength(), requestDecoder.sbeSchemaVersion());

        final ControlMessageType messageType = requestDecoder.messageType();
        lastRequestMessageType(messageType);

        final int partitionId = requestDecoder.partitionId();

        ensureBufferCapacity(requestDecoder.dataLength());
        requestDecoder.getData(requestBuffer, 0, requestDecoder.dataLength());

        final ControlMessageHandler handler = handlersByTypeId.get(messageType.value());
        if (handler != null)
        {
            final ActorFuture<Void> handleFuture = handler.handle(partitionId, requestBuffer, eventMetada);
            actor.runOnCompletion(handleFuture, ((aVoid, throwable) ->
            {
                if (throwable != null)
                {
                    LOG.error("Could not process control message request successfully. A response may not be sent.", throwable);
                }
            }));

            actor.runDelayed(Duration.ofMillis(requestTimeoutInMillis), () ->
            {
                if (!handleFuture.isDone())
                {
                    // timeout
                    errorResponseWriter
                        .errorCode(ErrorCode.REQUEST_TIMEOUT)
                        .errorMessage("Timeout while handle control message.")
                        .failedRequest(requestBuffer, 0, requestBuffer.capacity())
                        .tryWriteResponseOrLogFailure(eventMetada.getRequestStreamId(), eventMetada.getRequestId());

                    handleFuture.completeExceptionally(new TimeoutException());
                }
            });
        }
        else
        {
            actor.call(() ->
            {
                errorResponseWriter
                    .errorCode(ErrorCode.MESSAGE_NOT_SUPPORTED)
                    .errorMessage("Cannot handle control message with type '%s'.", getLastRequestMessageType().name())
                    .failedRequest(requestBuffer, 0, requestBuffer.capacity())
                    .tryWriteResponseOrLogFailure(eventMetada.getRequestStreamId(), eventMetada.getRequestId());
            });
        }

        return FragmentHandler.CONSUME_FRAGMENT_RESULT;
    }

    protected void ensureBufferCapacity(int length)
    {
        byte[] raw = requestBuffer.byteArray();

        if (length <= raw.length)
        {
            Arrays.fill(raw, (byte) 0);
        }
        else
        {
            raw = new byte[length];
        }

        requestBuffer.wrap(raw, 0, length);
    }

    public void lastRequestMessageType(ControlMessageType messageType)
    {
        this.lastRequestMessageType = messageType;
    }

    public ControlMessageType getLastRequestMessageType()
    {
        return lastRequestMessageType;
    }

}
