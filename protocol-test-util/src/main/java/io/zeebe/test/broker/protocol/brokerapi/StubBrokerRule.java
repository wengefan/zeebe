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
package io.zeebe.test.broker.protocol.brokerapi;

import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.test.broker.protocol.brokerapi.data.BrokerPartitionState;
import io.zeebe.test.broker.protocol.brokerapi.data.Topology;
import io.zeebe.test.broker.protocol.brokerapi.data.TopologyBroker;
import io.zeebe.test.util.collection.MapFactoryBuilder;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.Transports;
import io.zeebe.util.sched.ZbActorScheduler;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.zeebe.test.broker.protocol.clientapi.ClientApiRule.DEFAULT_TOPIC_NAME;

public class StubBrokerRule extends ExternalResource
{

    public static final String TEST_TOPIC_NAME = DEFAULT_TOPIC_NAME;
    public static final int TEST_PARTITION_ID = 99;


    protected ZbActorScheduler scheduler;

    protected final String host;
    protected final int port;

    protected ServerTransport transport;
    protected Dispatcher sendBuffer;

    protected StubResponseChannelHandler channelHandler;
    protected MsgPackHelper msgPackHelper;
    private InetSocketAddress bindAddr;

    protected AtomicReference<Topology> currentTopology = new AtomicReference<>();

    public StubBrokerRule()
    {
        this("127.0.0.1", 51015);
    }

    public StubBrokerRule(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    @Override
    protected void before() throws Throwable
    {
        msgPackHelper = new MsgPackHelper();

        final int numThreads = Math.min(1, Runtime.getRuntime().availableProcessors() - 1);
        scheduler = new ZbActorScheduler(numThreads);
        scheduler.start();

        sendBuffer = Dispatchers.create("send-buffer")
            .actorScheduler(scheduler)
            .bufferSize(1024 * 1024)
            .build();

        channelHandler = new StubResponseChannelHandler(msgPackHelper);
        bindAddr = new InetSocketAddress(host, port);

        currentTopology.set(new Topology()
            .addLeader(host, port, TEST_TOPIC_NAME, TEST_PARTITION_ID)
            .addLeader(host, port, Protocol.SYSTEM_TOPIC, Protocol.SYSTEM_PARTITION));

        stubTopologyRequest();

        bindTransport();
    }

    @Override
    protected void after()
    {
        if (transport != null)
        {
            closeTransport();
        }
        if (sendBuffer != null)
        {
            sendBuffer.close();
        }
        if (scheduler != null)
        {
            scheduler.stop();
        }
    }

    public void interruptAllServerChannels()
    {
        transport.interruptAllChannels();
    }

    public void closeTransport()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
        }
        else
        {
            throw new RuntimeException("transport not open");
        }
    }

    public void bindTransport()
    {
        if (transport == null)
        {
            transport = Transports.newServerTransport()
                    .bindAddress(bindAddr)
                    .scheduler(scheduler)
                    .sendBuffer(sendBuffer)
                    .build(null, channelHandler);
        }
        else
        {
            throw new RuntimeException("transport already open");
        }
    }

    public ServerTransport getTransport()
    {
        return transport;
    }

    public MapFactoryBuilder<ExecuteCommandRequest, ExecuteCommandResponseBuilder> onWorkflowRequestRespondWith(long key)
    {
        return onWorkflowRequestRespondWith(TEST_TOPIC_NAME, TEST_PARTITION_ID, key);
    }

    public MapFactoryBuilder<ExecuteCommandRequest, ExecuteCommandResponseBuilder> onWorkflowRequestRespondWith(final String topicName, final int partitionId, final long key)
    {
        final MapFactoryBuilder<ExecuteCommandRequest, ExecuteCommandResponseBuilder> eventType = onExecuteCommandRequest(ecr -> ecr.eventType() == EventType.WORKFLOW_INSTANCE_EVENT)
            .respondWith()
            .partitionId(partitionId)
            .key(key)
            .event()
            .allOf((r) -> r.getCommand());

        return eventType;
    }

    public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest()
    {
        return onExecuteCommandRequest((r) -> true);
    }

    public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(Predicate<ExecuteCommandRequest> activationFunction)
    {
        return new ExecuteCommandResponseTypeBuilder(
                channelHandler::addExecuteCommandRequestStub,
                activationFunction,
                msgPackHelper
                );
    }

    public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(EventType eventType, String eventStatus)
    {
        return onExecuteCommandRequest(ecr -> ecr.eventType() == eventType && eventStatus.equals(ecr.getCommand().get("state")));
    }

    public ExecuteCommandResponseTypeBuilder onExecuteCommandRequest(
            int partitionId,
            EventType eventType,
            String eventStatus)
    {
        return onExecuteCommandRequest(ecr ->
            ecr.partitionId() == partitionId &&
            ecr.eventType() == eventType &&
            eventStatus.equals(ecr.getCommand().get("state")));
    }

    public ControlMessageResponseTypeBuilder onControlMessageRequest()
    {
        return onControlMessageRequest((r) -> true);
    }

    public ControlMessageResponseTypeBuilder onControlMessageRequest(Predicate<ControlMessageRequest> activationFunction)
    {
        return new ControlMessageResponseTypeBuilder(
                channelHandler::addControlMessageRequestStub,
                activationFunction,
                msgPackHelper
                );
    }

    public List<ControlMessageRequest> getReceivedControlMessageRequests()
    {
        return channelHandler.getReceivedControlMessageRequests();
    }

    public List<ExecuteCommandRequest> getReceivedCommandRequests()
    {
        return channelHandler.getReceivedCommandRequests();
    }

    public List<Object> getAllReceivedRequests()
    {
        return channelHandler.getAllReceivedRequests();
    }

    public SubscribedEventBuilder newSubscribedEvent()
    {
        return new SubscribedEventBuilder(msgPackHelper, transport);
    }

    public void stubTopologyRequest()
    {
        onTopologyRequest()
            .respondWith()
            .data()
                .put("brokers", r -> currentTopology.get().getBrokers())
                .done()
            .register();

        // assuming that topology and partitions request are consistent
        onControlMessageRequest(r -> r.messageType() == ControlMessageType.REQUEST_PARTITIONS && r.partitionId() == Protocol.SYSTEM_PARTITION)
            .respondWith()
            .data()
                .put("partitions", r ->
                {
                    final Topology topology = currentTopology.get();
                    final List<Map<String, Object>> partitions = new ArrayList<>();
                    for (TopologyBroker broker : topology.getBrokers())
                    {
                        final List<BrokerPartitionState> brokerPartitionStates = broker.getPartitions();
                        for (BrokerPartitionState brokerPartitionState : brokerPartitionStates)
                        {
                            final Map<String, Object> partition = new HashMap<>();
                            partition.put("topic", brokerPartitionState.getTopicName());
                            partition.put("id", brokerPartitionState.getPartitionId());
                            partitions.add(partition);
                        }
                    }
                    return partitions;
                })
                .done()
            .register();
    }

    public ControlMessageResponseTypeBuilder onTopologyRequest()
    {
        return onControlMessageRequest(r -> r.messageType() == ControlMessageType.REQUEST_TOPOLOGY);
    }

    public void addTopic(String topic, int partition)
    {
        final Topology newTopology = new Topology(currentTopology.get());

        newTopology.addLeader(host, port, topic, partition);
        currentTopology.set(newTopology);
    }

    public void addSystemTopic()
    {
        addTopic(Protocol.SYSTEM_TOPIC, Protocol.SYSTEM_PARTITION);
    }

    public void setCurrentTopology(Topology currentTopology)
    {
        this.currentTopology.set(currentTopology);
    }

    public void clearTopology()
    {
        currentTopology.set(new Topology());
    }

    public void stubTopicSubscriptionApi(long initialSubscriberKey)
    {
        final AtomicLong subscriberKeyProvider = new AtomicLong(initialSubscriberKey);
        final AtomicLong subscriptionKeyProvider = new AtomicLong(0);

        onExecuteCommandRequest(EventType.SUBSCRIBER_EVENT, "SUBSCRIBE")
            .respondWith()
            .key((r) -> subscriberKeyProvider.getAndIncrement())
            .event()
                .allOf((r) -> r.getCommand())
                .put("state", "SUBSCRIBED")
                .done()
            .register();

        onControlMessageRequest((r) -> r.messageType() == ControlMessageType.REMOVE_TOPIC_SUBSCRIPTION)
            .respondWith()
            .data()
                .allOf((r) -> r.getData())
                .done()
            .register();

        onExecuteCommandRequest(EventType.SUBSCRIPTION_EVENT, "ACKNOWLEDGE")
            .respondWith()
            .key((r) -> subscriptionKeyProvider.getAndIncrement())
            .partitionId((r) -> r.partitionId())
            .event()
                .allOf((r) -> r.getCommand())
                .put("state", "ACKNOWLEDGED")
                .done()
            .register();
    }

    public void stubTaskSubscriptionApi(long initialSubscriberKey)
    {
        final AtomicLong subscriberKeyProvider = new AtomicLong(initialSubscriberKey);

        onControlMessageRequest((r) -> r.messageType() == ControlMessageType.ADD_TASK_SUBSCRIPTION)
            .respondWith()
            .data()
                .allOf((r) -> r.getData())
                .put("subscriberKey", (r) -> subscriberKeyProvider.getAndIncrement())
                .done()
            .register();

        onControlMessageRequest((r) -> r.messageType() == ControlMessageType.REMOVE_TASK_SUBSCRIPTION)
            .respondWith()
            .data()
                .allOf((r) -> r.getData())
                .done()
            .register();

        onControlMessageRequest((r) -> r.messageType() == ControlMessageType.INCREASE_TASK_SUBSCRIPTION_CREDITS)
            .respondWith()
            .data()
                .allOf((r) -> r.getData())
                .done()
            .register();
    }

    public void pushTopicEvent(RemoteAddress remote, long subscriberKey, long key, long position)
    {
        pushTopicEvent(remote, subscriberKey, key, position, EventType.RAFT_EVENT);
    }

    public void pushTopicEvent(RemoteAddress remote, long subscriberKey, long key, long position, EventType eventType)
    {
        newSubscribedEvent()
            .partitionId(TEST_PARTITION_ID)
            .key(key)
            .position(position)
            .eventType(eventType)
            .subscriberKey(subscriberKey)
            .subscriptionType(SubscriptionType.TOPIC_SUBSCRIPTION)
            .event()
                .done()
            .push(remote);
    }

    public void pushTopicEvent(RemoteAddress remote, Consumer<SubscribedEventBuilder> eventConfig)
    {
        final SubscribedEventBuilder builder = newSubscribedEvent()
            .subscriptionType(SubscriptionType.TOPIC_SUBSCRIPTION);

        // defaults that the config can override
        builder.partitionId(1);
        builder.key(0);
        builder.position(0);
        builder.eventType(EventType.RAFT_EVENT);
        builder.subscriberKey(0);
        builder.partitionId(TEST_PARTITION_ID);
        builder.event().done();

        eventConfig.accept(builder);

        builder.push(remote);

    }

    public void pushLockedTask(RemoteAddress remote, long subscriberKey, long key, long position, String lockOwner, String taskType)
    {
        newSubscribedEvent()
            .partitionId(TEST_PARTITION_ID)
            .key(key)
            .position(position)
            .eventType(EventType.TASK_EVENT)
            .subscriberKey(subscriberKey)
            .subscriptionType(SubscriptionType.TASK_SUBSCRIPTION)
            .event()
                .put("type", taskType)
                .put("lockTime", 1000L)
                .put("lockOwner", lockOwner)
                .put("retries", 3)
                .put("payload", msgPackHelper.encodeAsMsgPack(new HashMap<>()))
                .put("state", "LOCKED")
                .done()
            .push(remote);
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }
}
