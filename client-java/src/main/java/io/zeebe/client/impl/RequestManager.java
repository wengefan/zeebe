package io.zeebe.client.impl;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.zeebe.client.clustering.impl.ClientTopologyManager;
import io.zeebe.client.clustering.impl.TopologyImpl;
import io.zeebe.client.cmd.BrokerErrorException;
import io.zeebe.client.cmd.ClientCommandRejectedException;
import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.event.Event;
import io.zeebe.client.impl.cmd.CommandImpl;
import io.zeebe.client.task.impl.ControlMessageRequest;
import io.zeebe.client.task.impl.ErrorResponseHandler;
import io.zeebe.protocol.clientapi.ErrorCode;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.transport.ClientOutput;
import io.zeebe.transport.ClientRequest;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletedActorFuture;

public class RequestManager
{
    protected final ClientOutput output;
    protected final ClientTopologyManager topologyManager;
    protected final ObjectMapper msgPackMapper;
    protected final Duration requestTimeout;
    protected final RequestDispatchStrategy dispatchStrategy;

    public RequestManager(
            ClientOutput output,
            ClientTopologyManager topologyManager,
            ObjectMapper msgPackMapper,
            Duration requestTimeout)
    {
        this.output = output;
        this.topologyManager = topologyManager;
        this.msgPackMapper = msgPackMapper;
        this.requestTimeout = requestTimeout;
        this.dispatchStrategy = new RoundRobinDispatchStrategy(topologyManager);
    }

    public <E extends Event> E execute(final CommandImpl<E> command)
    {
        return waitAndResolve(executeAsync(command));
    }

    public <E> E execute(ControlMessageRequest<E> controlMessage)
    {
        return waitAndResolve(executeAsync(controlMessage));
    }

    protected <E> Future<E> executeAsync(final RequestResponseHandler requestHandler)
    {
        final Supplier<ActorFuture<RemoteAddress>> remoteProvider = determineRemoteProvider(requestHandler);

        // TODO: make nicer
        final ErrorResponseHandler errorHandler = new ErrorResponseHandler();
        final Predicate<DirectBuffer> responseInspector = responseBuffer ->
        {
            final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
            headerDecoder.wrap(responseBuffer, 0);

            if (errorHandler.handlesResponse(headerDecoder))
            {
                errorHandler.wrap(responseBuffer, headerDecoder.encodedLength(), headerDecoder.blockLength(), headerDecoder.version());

                final ErrorCode errorCode = errorHandler.getErrorCode();
                return errorCode == ErrorCode.PARTITION_NOT_FOUND || errorCode == ErrorCode.REQUEST_TIMEOUT;
            }
            else
            {
                return false;
            }
        };

        // TODO: serialize the entire request here already
        final ActorFuture<ClientRequest> responseFuture =
                output.sendRequestWithRetry(remoteProvider, responseInspector, requestHandler, requestTimeout);

        return new ResponseFuture<>(responseFuture, requestHandler);
    }

    private Supplier<ActorFuture<RemoteAddress>> determineRemoteProvider(RequestResponseHandler requestHandler)
    {
        if (requestHandler.getTargetTopic() == null)
        {
            return () ->
            {
                final TopologyImpl topology = topologyManager.getTopology();
                return new CompletedActorFuture<>(topology.getRandomBroker());
            };
        }
        else
        {
            final int targetPartition;
            if (requestHandler.getTargetPartition() < 0)
            {
                int proposedPartition = dispatchStrategy.determinePartition(requestHandler.getTargetTopic());

                if (proposedPartition >= 0)
                {
                    targetPartition = proposedPartition;
                }
                else
                {
                    // TODO: man könnte diese Loop auch von einem Actor ausführen lassen,
                    // um so lange die Topology neuzuholen bis man einen Leader sieht
                    try
                    {
                        topologyManager.requestTopology().get(10, TimeUnit.SECONDS);
                        proposedPartition = dispatchStrategy.determinePartition(requestHandler.getTargetTopic());
                    }
                    catch (InterruptedException | ExecutionException | TimeoutException e)
                    {
                        proposedPartition = -1;
                    }

                    targetPartition = proposedPartition;
                }
                if (targetPartition < 0)
                {
                    throw new ClientException("Cannot determine a partition for topic " + requestHandler.getTargetTopic());
                }
                else
                {
                    requestHandler.onSelectedPartition(targetPartition);
                }
            }
            else
            {
                targetPartition = requestHandler.getTargetPartition();
            }

            return () ->
            {
                final TopologyImpl topology = topologyManager.getTopology();
                return new CompletedActorFuture<>(topology.getLeaderForPartition(targetPartition));
            };
        }

    }

    public <E> Future<E> executeAsync(final ControlMessageRequest<E> controlMessage)
    {
        final ControlMessageRequestHandler requestHandler = new ControlMessageRequestHandler(msgPackMapper, controlMessage);
        return executeAsync(requestHandler);
    }

    public <E extends Event> Future<E> executeAsync(final CommandImpl<E> command)
    {
        final CommandRequestHandler requestHandler = new CommandRequestHandler(msgPackMapper, command);
        return executeAsync(requestHandler);
    }

    protected <E> E waitAndResolve(Future<E> future)
    {
        try
        {
            return future.get();
        }
        catch (final InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting for command result", e);
        }
        catch (final ExecutionException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException)
            {
                throw ((ClientException) cause).newInCurrentContext();
            }
            else
            {
                throw new ClientException("Could not make request", e);
            }
        }
    }

    protected static class ResponseFuture<E> implements Future<E>
    {
        protected final Future<ClientRequest> transportFuture;
        protected final RequestResponseHandler responseHandler;
        protected final ErrorResponseHandler errorHandler = new ErrorResponseHandler();
        protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

        protected E result = null;
        protected ExecutionException failure = null;

        ResponseFuture(Future<ClientRequest> transportFuture, RequestResponseHandler responseHandler)
        {
            this.transportFuture = transportFuture;
            this.responseHandler = responseHandler;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return transportFuture.isDone();
        }

        protected void ensureResponseAvailable(long timeout, TimeUnit unit)
        {
            if (result != null || failure != null)
            {
                return;
            }

            final ClientRequest resolvedRequest;
            final DirectBuffer responseContent;
            try
            {
                resolvedRequest = transportFuture.get(timeout, unit);
                responseContent = resolvedRequest.get();
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                // TODO: im nicht-async-Fall können wir vermeiden eine ExecutionException einzupacken
                // und dann im RequestManager wieder auszupacken
                failWith(new ClientException("Could not complete request", e));
                return;
            }

            headerDecoder.wrap(responseContent, 0);

            if (responseHandler.handlesResponse(headerDecoder))
            {
                try
                {
                    this.result = (E) responseHandler.getResult(responseContent,
                            headerDecoder.encodedLength(),
                            headerDecoder.blockLength(),
                            headerDecoder.version());
                    return;
                }
                catch (ClientCommandRejectedException e)
                {
                    failWith(e);
                    return;
                }
                catch (Exception e)
                {
                    failWith(new ClientException("Unexpected exception during response handling", e));
                    return;
                }
            }
            else if (errorHandler.handlesResponse(headerDecoder))
            {
                final ErrorCode errorCode = errorHandler.getErrorCode();

                if (errorCode != ErrorCode.NULL_VAL)
                {
                    try
                    {
                        final String errorMessage = BufferUtil.bufferAsString(errorHandler.getErrorMessage());
                        failWith(new BrokerErrorException(errorCode, errorMessage));
                        return;
                    }
                    catch (final Exception e)
                    {
                        failWith(new BrokerErrorException(errorCode, "Unable to parse error message from response: " + e.getMessage()));
                        return;
                    }
                }
                else
                {
                    failWith(new ClientException("Unknown error during request execution"));
                    return;
                }
            }
            else
            {
                failWith(new ClientException("Unexpected response format"));
                return;
            }
        }

        protected void failWith(Exception e)
        {
            this.failure = new ExecutionException(e);
        }

        @Override
        public E get() throws InterruptedException, ExecutionException
        {
            try
            {
                return get(1, TimeUnit.DAYS);
            }
            catch (TimeoutException e)
            {
                // TODO: vll kann man vermeiden get mit timeout in diesem Fall aufzurufen
                throw new RuntimeException(e);
            }
        }

        @Override
        public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            ensureResponseAvailable(timeout, unit);

            if (result != null)
            {
                return result;
            }
            else
            {
                throw failure;
            }
        }

    }

}
