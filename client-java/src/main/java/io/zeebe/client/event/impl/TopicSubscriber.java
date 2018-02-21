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
package io.zeebe.client.event.impl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import io.zeebe.client.task.impl.subscription.SubscriptionManager;
import io.zeebe.client.task.impl.subscription.EventSubscriber;
import io.zeebe.client.task.impl.subscription.EventSubscriberGroup;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.CheckedConsumer;
import io.zeebe.util.sched.future.ActorFuture;

public class TopicSubscriber extends EventSubscriber
{

    protected static final int MAX_HANDLING_RETRIES = 2;

    protected final TopicClientImpl client;

    protected AtomicBoolean processingFlag = new AtomicBoolean(false);
    protected volatile long lastProcessedEventPosition;
    protected long lastAcknowledgedPosition;

    protected final TopicSubscriptionSpec subscription;

    protected final Function<CheckedConsumer<GeneralEventImpl>, CheckedConsumer<GeneralEventImpl>> eventHandlerAdapter;

    public TopicSubscriber(
            TopicClientImpl client,
            TopicSubscriptionSpec subscription,
            long subscriberKey,
            RemoteAddress eventSource,
            int partitionId,
            EventSubscriberGroup group,
            SubscriptionManager acquisition)
    {
        super(subscriberKey, partitionId, subscription.getPrefetchCapacity(), eventSource, group, acquisition);
        this.subscription = subscription;
        this.client = client;
        this.lastProcessedEventPosition = subscription.getStartPosition(partitionId);
        this.lastAcknowledgedPosition = subscription.getStartPosition(partitionId);

        if (subscription.isManaged())
        {
            eventHandlerAdapter = h -> h
                .andThen(this::recordProcessedEvent)
                .andOnExceptionRetry(MAX_HANDLING_RETRIES, this::logRetry)
                .andOnException(this::logExceptionAndClose);
        }
        else
        {
            eventHandlerAdapter = h -> h
                .andThen(this::recordProcessedEvent)
                .andOnException(this::logExceptionAndPropagate);
        }

    }

    public int pollEvents(CheckedConsumer<GeneralEventImpl> consumer)
    {
        return super.pollEvents(eventHandlerAdapter.apply(consumer));
    }

    protected void logExceptionAndClose(GeneralEventImpl event, Exception e)
    {
        logEventHandlingError(e, event, "Closing subscription.");
        disable();

        acquisition.closeGroup(group);
    }

    protected void logExceptionAndPropagate(GeneralEventImpl event, Exception e)
    {
        logEventHandlingError(e, event, "Propagating exception to caller.");
        throw new RuntimeException(e);
    }

    protected void logRetry(GeneralEventImpl event, Exception e)
    {
        logEventHandlingError(e, event, "Retrying.");
    }

    @Override
    protected ActorFuture<Void> requestSubscriptionClose()
    {
        // TODO: das hier muss woanders hin bzw. mit dem Future komponiert werden
        acknowledgeLastProcessedEvent();

        return client.closeTopicSubscription(partitionId, subscriberKey).executeAsync();
    }

    @Override
    protected void requestEventSourceReplenishment(int eventsProcessed)
    {
        acknowledgeLastProcessedEvent();
    }

    protected void acknowledgeLastProcessedEvent()
    {

        // note: it is important we read lastProcessedEventPosition only once
        //   as it can be changed concurrently by an executor thread
        final long positionToAck = lastProcessedEventPosition;

        if (positionToAck > lastAcknowledgedPosition)
        {
            // TODO: what to do on error here? close the group (but only if it is not already closing)
            client.acknowledgeEvent(subscription.getTopic(), partitionId)
                .subscriptionName(subscription.getName())
                .ackPosition(positionToAck)
                .executeAsync();

            lastAcknowledgedPosition = positionToAck;
        }
    }

    protected void recordProcessedEvent(GeneralEventImpl event)
    {
        this.lastProcessedEventPosition = event.getMetadata().getPosition();
    }

    protected void logEventHandlingError(Exception e, GeneralEventImpl event, String resolution)
    {
        LOGGER.error(LOG_MESSAGE_PREFIX + "Unhandled exception during handling of event {}.{}", this, event, resolution, e);
    }

    @Override
    public String getTopicName()
    {
        return subscription.getTopic();
    }

    @Override
    public String toString()
    {
        return "TopicSubscriber[topic=" + subscription.getTopic() + ", partition=" + partitionId + ", name=" + subscription.getName() + ", subscriberKey=" + subscriberKey + "]";
    }

}
