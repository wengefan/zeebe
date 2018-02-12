package io.zeebe.client.clustering.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.agrona.DirectBuffer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.zeebe.client.clustering.Topology;
import io.zeebe.client.cmd.BrokerErrorException;
import io.zeebe.client.impl.ControlMessageRequestHandler;
import io.zeebe.protocol.clientapi.ErrorResponseDecoder;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.transport.ClientOutput;
import io.zeebe.transport.ClientRequest;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class ClientTopologyManager extends ZbActor
{
    /**
     * Interval in which the topology is refreshed even if the client is idle
     */
    public static final Duration MAX_REFRESH_INTERVAL_MILLIS = Duration.ofSeconds(10);

    /**
     * Shortest possible interval in which the topology is refreshed,
     * even if the client is constantly making new requests that require topology refresh
     */
    public static final Duration MIN_REFRESH_INTERVAL_MILLIS = Duration.ofMillis(300);

    protected ClientOutput output;
    protected ClientTransport transport;
    protected TopologyImpl topology;

    protected List<CompletableActorFuture<Topology>> nextTopologyFutures = new ArrayList<>();
    protected ControlMessageRequestHandler requestWriter;

    protected int refreshAttempt = 0;

    public ClientTopologyManager(ClientTransport transport, ObjectMapper objectMapper, RemoteAddress initialContact)
    {
        this.transport = transport;
        this.output = transport.getOutput();

        this.requestWriter = new ControlMessageRequestHandler(objectMapper);
        this.requestWriter.configure(new RequestTopologyCmdImpl(null));
    }

    @Override
    protected void onActorStarted()
    {
        actor.run(this::refreshTopology);
        actor.runAtFixedRate(MIN_REFRESH_INTERVAL_MILLIS,
            () ->
            {
                if (!nextTopologyFutures.isEmpty())
                {
                    refreshTopology();
                }
            });
    }

    @Override
    protected void onActorClosing()
    {
        // TODO Auto-generated method stub
    }

    public ActorFuture<Topology> getTopology()
    {
        return actor.call(() -> topology);
    }

    public ActorFuture<Topology> requestTopology()
    {
        final CompletableActorFuture<Topology> future = new CompletableActorFuture<>();

        actor.call(() ->
        {
            nextTopologyFutures.add(future);
            // TODO: trigger next topology refresh
        });

        return future;
    }

    public void provideTopology(TopologyResponse topology)
    {
        actor.call(() ->
        {
            // TODO: not sure we should complete the refresh futures in this case,
            //   as the response could be older than the time when the future was submitted
            onNewTopology(topology);
        });
    }

    protected void refreshTopology()
    {
        final RemoteAddress endpoint = topology.getRandomBroker();
        // TODO: hier könnte man jetzt den Endpoint mit jedem Fehlschlag wechseln
        final ActorFuture<ClientRequest> request = output.sendRequestWithRetry(endpoint, requestWriter, Duration.ofSeconds(1));

        if (request != null)
        {
            refreshAttempt++;
            actor.await(request, this::handleResponse);
            actor.runDelayed(MAX_REFRESH_INTERVAL_MILLIS, scheduleIdleRefresh());
        }
        else
        {
            actor.run(this::refreshTopology);
            actor.yield();
        }
    }

    /**
     * Only schedules topology refresh if there was no refresh attempt in the last ten seconds
     */
    protected Runnable scheduleIdleRefresh()
    {
        final int currentAttempt = refreshAttempt;

        return () ->
        {
            // if no topology refresh attempt was made in the meantime
            if (currentAttempt == refreshAttempt)
            {
                actor.run(this::refreshTopology);
            }
        };
    }

    protected void handleResponse(ClientRequest result, Throwable t)
    {
        if (t == null)
        {
            try
            {
                final TopologyResponse topologyResponse = decodeTopology(result.get());
                onNewTopology(topologyResponse);
            }
            catch (InterruptedException | ExecutionException e)
            {
                failRefreshFutures(e);
            }
        }
        else
        {
            failRefreshFutures(t);
        }
    }

    protected void onNewTopology(TopologyResponse topologyResponse)
    {
        this.topology = new TopologyImpl(topologyResponse, transport::registerRemoteAddress);
        completeRefreshFutures();
    }

    protected void completeRefreshFutures()
    {
        nextTopologyFutures.forEach(f -> f.complete(topology));
        nextTopologyFutures.clear();
    }

    protected void failRefreshFutures(Throwable t)
    {
        nextTopologyFutures.forEach(f -> f.completeExceptionally("Could not refresh topology", t));
        nextTopologyFutures.clear();
    }

    // TODO: encapsulate this and the following method somewhere
    protected final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    protected final ErrorResponseDecoder errorResponseDecoder = new ErrorResponseDecoder();

    protected TopologyResponse decodeTopology(DirectBuffer encodedTopology)
    {
        messageHeaderDecoder.wrap(encodedTopology, 0);

        final int blockLength = messageHeaderDecoder.blockLength();
        final int version = messageHeaderDecoder.version();

        final int responseMessageOffset = messageHeaderDecoder.encodedLength();

        if (requestWriter.handlesResponse(messageHeaderDecoder))
        {
            try
            {
                return (TopologyResponse) requestWriter.getResult(encodedTopology, responseMessageOffset, blockLength, version);
            }
            catch (final Exception e)
            {
                throw new RuntimeException("Unable to parse topic list from broker response", e);
            }
        }
        else if (messageHeaderDecoder.schemaId() == ErrorResponseDecoder.SCHEMA_ID && messageHeaderDecoder.templateId() == ErrorResponseDecoder.TEMPLATE_ID)
        {
            errorResponseDecoder.wrap(encodedTopology, 0, blockLength, version);
            throw new BrokerErrorException(errorResponseDecoder.errorCode(), errorResponseDecoder.errorData());
        }
        else
        {
            throw new RuntimeException(String.format("Unexpected response format. Schema %s and template %s.", messageHeaderDecoder.schemaId(), messageHeaderDecoder.templateId()));
        }
    }
}
