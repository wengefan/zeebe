/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.impl;

import static io.zeebe.client.ClientProperties.CLIENT_MAXREQUESTS;
import static io.zeebe.client.ClientProperties.CLIENT_REQUEST_TIMEOUT_SEC;
import static io.zeebe.client.ClientProperties.CLIENT_SENDBUFFER_SIZE;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.zeebe.client.ClientProperties;
import io.zeebe.client.WorkflowsClient;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.clustering.impl.ClientTopologyManager;
import io.zeebe.client.clustering.impl.RequestTopologyCmdImpl;
import io.zeebe.client.clustering.impl.TopologyResponse;
import io.zeebe.client.cmd.Request;
import io.zeebe.client.event.impl.TopicClientImpl;
import io.zeebe.client.impl.data.MsgPackConverter;
import io.zeebe.client.impl.data.MsgPackMapper;
import io.zeebe.client.task.impl.subscription.EventAcquisition2;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ClientTransportBuilder;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.Transports;
import io.zeebe.util.sched.ZbActorScheduler;
import io.zeebe.util.sched.clock.ActorClock;

public class ZeebeClientImpl implements ZeebeClient
{

    public static final Logger LOG = Loggers.CLIENT_LOGGER;

    public static final String VERSION;

    static
    {
        final String version = ZeebeClient.class.getPackage().getImplementationVersion();
        VERSION = version != null ? version : "development";
    }

    protected final Properties initializationProperties;

    protected Dispatcher dataFrameReceiveBuffer;
    protected Dispatcher sendBuffer;
    protected ZbActorScheduler scheduler;

    protected ClientTransport transport;

    protected final MsgPackMapper msgPackMapper;

//    protected SubscriptionManager subscriptionManager;

    protected final ClientTopologyManager topologyManager;
    protected final RequestManager apiCommandManager;

    protected final MsgPackConverter msgPackConverter;

    protected boolean isClosed;

    protected EventAcquisition2 eventAcquisition;

    private final int subscriptionPrefetchCapacity;

    private final int numExecutionThreads;


    public ZeebeClientImpl(final Properties properties)
    {
        this(properties, null);
    }

    public ZeebeClientImpl(final Properties properties, ActorClock actorClock)
    {
        LOG.info("Version: {}", VERSION);

        ClientProperties.setDefaults(properties);
        this.initializationProperties = properties;

        final SocketAddress contactPoint = SocketAddress.from(properties.getProperty(ClientProperties.BROKER_CONTACTPOINT));

        final int maxRequests = Integer.parseInt(properties.getProperty(CLIENT_MAXREQUESTS));
        final int sendBufferSize = Integer.parseInt(properties.getProperty(CLIENT_SENDBUFFER_SIZE));

        // TODO: init properly
        final int numThreads = Math.min(1, Runtime.getRuntime().availableProcessors() - 1);
        this.scheduler = new ZbActorScheduler(numThreads, actorClock);
        this.scheduler.start();

        dataFrameReceiveBuffer = Dispatchers.create("receive-buffer")
            .bufferSize(1024 * 1024 * sendBufferSize)
            .modePubSub()
            .frameMaxLength(1024 * 1024)
            .actorScheduler(scheduler)
            .build();

        sendBuffer = Dispatchers.create("send-buffer")
            .actorScheduler(scheduler)
            .bufferSize(1024 * 1024 * sendBufferSize)
            .build();

        final ClientTransportBuilder transportBuilder = Transports.newClientTransport()
            .messageMaxLength(1024 * 1024)
            .messageReceiveBuffer(dataFrameReceiveBuffer)
            .requestPoolSize(maxRequests + 16)
            .scheduler(scheduler)
            .sendBuffer(sendBuffer);

        if (properties.containsKey(ClientProperties.CLIENT_TCP_CHANNEL_KEEP_ALIVE_PERIOD))
        {
            final long keepAlivePeriod = Long.parseLong(properties.getProperty(ClientProperties.CLIENT_TCP_CHANNEL_KEEP_ALIVE_PERIOD));
            transportBuilder.keepAlivePeriod(Duration.ofMillis(keepAlivePeriod));
        }

        transport = transportBuilder.build();

        msgPackConverter = new MsgPackConverter();

        final MessagePackFactory messagePackFactory = new MessagePackFactory()
                .setReuseResourceInGenerator(false)
                .setReuseResourceInParser(false);
        final ObjectMapper objectMapper = new ObjectMapper(messagePackFactory);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        final InjectableValues injectionContext = new InjectableValues.Std()
                .addValue(MsgPackConverter.class, msgPackConverter);
        objectMapper.setInjectableValues(injectionContext);

        this.msgPackMapper = new MsgPackMapper(objectMapper);


        numExecutionThreads = Integer.parseInt(properties.getProperty(ClientProperties.CLIENT_TASK_EXECUTION_THREADS));
        subscriptionPrefetchCapacity = Integer.parseInt(properties.getProperty(ClientProperties.CLIENT_TOPIC_SUBSCRIPTION_PREFETCH_CAPACITY));

        final Duration requestTimeout = Duration.ofSeconds(Long.parseLong(properties.getProperty(CLIENT_REQUEST_TIMEOUT_SEC)));

        final RemoteAddress initialContactPoint = transport.registerRemoteAddress(contactPoint);

        topologyManager = new ClientTopologyManager(transport, objectMapper, initialContactPoint);
        scheduler.submitActor(topologyManager);

//        subscriptionManager = new SubscriptionManager(
//                this,
//                numExecutionThreads,
//                prefetchCapacity);
//        transport.registerChannelListener(subscriptionManager);

        apiCommandManager = new RequestManager(
                transport.getOutput(),
                topologyManager,
                objectMapper,
                requestTimeout);

        this.eventAcquisition = new EventAcquisition2(this);
        this.transport.registerChannelListener(eventAcquisition);
        this.scheduler.submitActor(eventAcquisition);

        // TODO: implement reopen-Feature
//        transport.registerChannelListener(eventAcquisition);

//        subscriptionManager.start();
    }

    @Override
    public void close()
    {
        if (isClosed)
        {
            return;
        }

        isClosed = true;

//        subscriptionManager.closeAllSubscribers();
//        subscriptionManager.stop();

//        subscriptionManager.close();

        eventAcquisition.close().join();

        try
        {
            transport.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            dataFrameReceiveBuffer.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            sendBuffer.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            scheduler.stop().get(15, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Could not shutdown client successfully", e);
        }
    }

    @Override
    public Request<TopologyResponse> requestTopology()
    {
        return new RequestTopologyCmdImpl(apiCommandManager, topologyManager);
    }

    @Override
    public TasksClientImpl tasks()
    {
        return new TasksClientImpl(this);
    }

    @Override
    public WorkflowsClient workflows()
    {
        return new WorkflowsClientImpl(this);
    }

    @Override
    public TopicClientImpl topics()
    {
        return new TopicClientImpl(this);
    }

    public RequestManager getCommandManager()
    {
        return apiCommandManager;
    }

    public ClientTopologyManager getTopologyManager()
    {
        return topologyManager;
    }

    public MsgPackMapper getMsgPackMapper()
    {
        return msgPackMapper;
    }

    public Properties getInitializationProperties()
    {
        return initializationProperties;
    }

    public ClientTransport getTransport()
    {
        return transport;
    }

    public MsgPackConverter getMsgPackConverter()
    {
        return msgPackConverter;
    }

    public ZbActorScheduler getScheduler()
    {
        return scheduler;
    }

    public EventAcquisition2 getEventAcquisition()
    {
        return eventAcquisition;
    }

    public int getSubscriptionPrefetchCapacity()
    {
        return subscriptionPrefetchCapacity;
    }

    public int getNumExecutionThreads()
    {
        return numExecutionThreads;
    }
}
