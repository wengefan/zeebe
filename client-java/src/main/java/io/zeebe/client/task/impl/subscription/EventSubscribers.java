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
package io.zeebe.client.task.impl.subscription;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;

import io.zeebe.client.impl.Loggers;

@SuppressWarnings("rawtypes")
public class EventSubscribers
{
    protected static final Logger LOGGER = Loggers.SUBSCRIPTION_LOGGER;

    // partitionId => subscriberKey => subscription (subscriber keys are not guaranteed to be globally unique)
    protected Int2ObjectHashMap<Long2ObjectHashMap<EventSubscriber>> subscribers = new Int2ObjectHashMap<>();

    protected final List<EventSubscriberGroup2> pollableSubscriberGroups = new CopyOnWriteArrayList<>();
    protected final List<EventSubscriberGroup2> managedSubscriberGroups = new CopyOnWriteArrayList<>();

    public void addGroup(final EventSubscriberGroup2 subscription)
    {
        if (subscription.isManagedGroup())
        {
            addManagedGroup(subscription);
        }
        else
        {
            addPollableGroup(subscription);
        }
    }

    protected void addPollableGroup(final EventSubscriberGroup2 subscription)
    {
        this.pollableSubscriberGroups.add(subscription);
    }

    protected void addManagedGroup(final EventSubscriberGroup2 subscription)
    {
        this.managedSubscriberGroups.add(subscription);
    }

    public void closeAllGroups()
    {
        for (final EventSubscriberGroup2 group : pollableSubscriberGroups)
        {
            closeGroup(group);
        }

        for (final EventSubscriberGroup2 group : managedSubscriberGroups)
        {
            closeGroup(group);
        }
    }

    protected void closeGroup(EventSubscriberGroup2 group)
    {
        try
        {
            group.close();
        }
        catch (final Exception e)
        {
            LOGGER.error("Unable to close subscriber group {}", group, e);
        }
    }

//    public void reopenSubscribersForRemote(RemoteAddress remoteAddress)
//    {
//        forAllDoConsume(managedSubscriberGroups, s ->
//        {
//            s.reopenSubscribersForRemote(remoteAddress);
//        });
//
//        forAllDoConsume(pollableSubscriberGroups, s ->
//        {
//            s.reopenSubscribersForRemote(remoteAddress);
//        });
//    }

    public void add(EventSubscriber subscriber)
    {
        this.subscribers
            .computeIfAbsent(subscriber.getPartitionId(), partitionId -> new Long2ObjectHashMap<>())
            .put(subscriber.getSubscriberKey(), subscriber);
    }

    public void remove(final EventSubscriber subscriber)
    {
        final int partitionId = subscriber.getPartitionId();

        final Long2ObjectHashMap<EventSubscriber> subscribersForPartition = subscribers.get(partitionId);
        if (subscribersForPartition != null)
        {
            subscribersForPartition.remove(subscriber.getSubscriberKey());

            if (subscribersForPartition.isEmpty())
            {
                subscribers.remove(partitionId);
            }
        }

    }

    public void removeGroup(EventSubscriberGroup group)
    {
        pollableSubscriberGroups.remove(group);
        managedSubscriberGroups.remove(group);
    }

    public EventSubscriber getSubscriber(final int partitionId, final long subscriberKey)
    {
        final Long2ObjectHashMap<EventSubscriber> subscribersForPartition = subscribers.get(partitionId);

        if (subscribersForPartition != null)
        {
            return subscribersForPartition.get(subscriberKey);
        }

        return null;
    }

    protected int forAllDo(List<EventSubscriberGroup2> groups, ToIntFunction<EventSubscriberGroup2> action)
    {
        int workCount = 0;

        for (EventSubscriberGroup2 group : groups)
        {
            workCount += action.applyAsInt(group);
        }

        return workCount;
    }

    protected void forAllDoConsume(List<EventSubscriberGroup2> groups, Consumer<EventSubscriberGroup2> action)
    {
        for (EventSubscriberGroup2 subscription : groups)
        {
            action.accept(subscription);
        }
    }

    public int pollManagedSubscribers()
    {
        return forAllDo(managedSubscriberGroups, s -> s.poll());
    }
}
