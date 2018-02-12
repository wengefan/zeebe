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
package io.zeebe.client.clustering.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import org.agrona.collections.Int2ObjectHashMap;

import io.zeebe.client.clustering.Topology;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;

/**
 * Immutable; Important because we hand this between actors. If this is supposed to become mutable, make sure
 * to make copies in the right places.
 */
public class TopologyImpl implements Topology
{
    protected final Int2ObjectHashMap<RemoteAddress> topicLeaders = new Int2ObjectHashMap<>();
    protected final List<RemoteAddress> brokers = new ArrayList<>();
    protected final Map<String, List<Integer>> partitionsByTopic = new HashMap<>();

    protected final Random randomBroker = new Random();

    public TopologyImpl(RemoteAddress endpoint)
    {
        brokers.add(endpoint);
    }

    public TopologyImpl(TopologyResponse topologyDto, Function<SocketAddress, RemoteAddress> remoteAddressProvider)
    {
        topologyDto.getBrokers()
            .stream()
            .forEach(b ->
            {
                final RemoteAddress remoteAddress = remoteAddressProvider.apply(b.getSocketAddress());
                brokers.add(remoteAddress);

                b.getPartitions().forEach(p ->
                {
                    final String topicName = p.getTopicName();
                    final int partitionId = p.getPartitionId();

                    if (p.isLeader())
                    {
                        topicLeaders.put(partitionId, remoteAddress);
                        partitionsByTopic
                            .computeIfAbsent(topicName, t -> new ArrayList<>())
                            .add(partitionId);
                    }
                });
            });
    }

    @Override
    public RemoteAddress getLeaderForPartition(int partition)
    {
        return topicLeaders.get(partition);
    }

    @Override
    public RemoteAddress getRandomBroker()
    {
        if (!brokers.isEmpty())
        {
            final int nextBroker = randomBroker.nextInt(brokers.size());
            return brokers.get(nextBroker);
        }
        else
        {
            throw new RuntimeException("Unable to select random broker from empty list");
        }
    }

    @Override
    public List<Integer> getPartitionsOfTopic(String topic)
    {
        return partitionsByTopic.get(topic);
    }

    @Override
    public String toString()
    {
        return "Topology{" +
            "topicLeaders=" + topicLeaders +
            ", brokers=" + brokers +
            '}';
    }

}
