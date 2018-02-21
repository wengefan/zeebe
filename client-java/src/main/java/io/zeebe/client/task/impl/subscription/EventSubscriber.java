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

import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.slf4j.Logger;

import io.zeebe.client.event.impl.GeneralEventImpl;
import io.zeebe.client.impl.Loggers;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.CheckedConsumer;
import io.zeebe.util.sched.future.ActorFuture;

public abstract class EventSubscriber
{
    protected static final Logger LOGGER = Loggers.SUBSCRIPTION_LOGGER;
    protected static final String LOG_MESSAGE_PREFIX = "Subscriber {}: ";

    // TODO: could become configurable in the future
    protected static final double REPLENISHMENT_THRESHOLD = 0.3d;

    protected final long subscriberKey;
    protected final ManyToManyConcurrentArrayQueue<GeneralEventImpl> pendingEvents;
    protected final int capacity;
    protected final SubscriptionManager acquisition;
    protected final EventSubscriberGroup group;

    protected RemoteAddress eventSource;
    protected int partitionId;

    protected final AtomicInteger eventsInProcessing = new AtomicInteger(0);
    protected final AtomicInteger eventsProcessedSinceLastReplenishment = new AtomicInteger(0);

    private volatile int state;

    private static final int STATE_OPEN = 0;
    private static final int STATE_DISABLED = 1; // required to immediately disable a subscriber and stop processing further events

    public EventSubscriber(
            long subscriberKey,
            int partitionId,
            int capacity,
            RemoteAddress eventSource,
            EventSubscriberGroup group,
            SubscriptionManager acquisition)
    {
        this.subscriberKey = subscriberKey;
        this.eventSource = eventSource;
        this.pendingEvents = new ManyToManyConcurrentArrayQueue<>(capacity);
        this.capacity = capacity;
        this.group = group;
        this.acquisition = acquisition;
        this.partitionId = partitionId;
        this.state = STATE_OPEN;
    }

    public RemoteAddress getEventSource()
    {
        return eventSource;
    }

    public boolean isOpen()
    {
        return state == STATE_OPEN;
    }

    public int size()
    {
        return pendingEvents.size();
    }

    protected boolean replenishEventSource()
    {
        final int eventsProcessed = eventsProcessedSinceLastReplenishment.get();
        final int remainingCapacity = capacity - eventsProcessed;

        final boolean requestReplenishment = remainingCapacity < capacity * REPLENISHMENT_THRESHOLD;

        if (requestReplenishment)
        {
            requestEventSourceReplenishment(eventsProcessed);
            eventsProcessedSinceLastReplenishment.addAndGet(-eventsProcessed);
        }

        return requestReplenishment;
    }

    public long getSubscriberKey()
    {
        return subscriberKey;
    }

    protected abstract void requestEventSourceReplenishment(int eventsProcessed);

    public boolean addEvent(GeneralEventImpl event)
    {
        final boolean added = this.pendingEvents.offer(event);

        if (!added)
        {
            LOGGER.warn(LOG_MESSAGE_PREFIX + "Cannot add any more events. Event queue saturated. Postponing event {}.",
                    this, event);
        }

        return added;
    }

    protected void resetProcessingState()
    {
        pendingEvents.clear();
        eventsInProcessing.set(0);
        eventsProcessedSinceLastReplenishment.set(0);
    }

    protected boolean hasEventsInProcessing()
    {
        return eventsInProcessing.get() > 0;
    }

    /**
     * Atomically stops this subscriber from handling any more events (aside from those currently in progress)
     */
    public void disable()
    {
        this.state = STATE_DISABLED;
    }

    protected int pollEvents(CheckedConsumer<GeneralEventImpl> pollHandler)
    {
        final int currentlyAvailableEvents = size();
        int handledEvents = 0;

        GeneralEventImpl event;

        // handledTasks < currentlyAvailableTasks avoids very long cycles that we spend in this method
        // in case the broker continuously produces new tasks
        while (handledEvents < currentlyAvailableEvents && isOpen())
        {
            event = pendingEvents.poll();
            if (event == null)
            {
                break;
            }

            eventsInProcessing.incrementAndGet();
            try
            {
                // Must first increment eventsInProcessing and only then check if the subscription
                // is still open. This avoids a race condition between the event handler executor
                // and the event acquisition checking if there are events in processing before closing a
                // subscription
                if (!isOpen())
                {
                    break;
                }

                handledEvents++;
                logHandling(event);

                try
                {
                    pollHandler.accept(event);
                }
                catch (Exception e)
                {
                    onUnhandledEventHandlingException(event, e);
                }
            }
            finally
            {
                eventsInProcessing.decrementAndGet();
                eventsProcessedSinceLastReplenishment.incrementAndGet();
            }
        }

        return handledEvents;
    }

    protected void logHandling(GeneralEventImpl event)
    {
        try
        {
            LOGGER.trace(LOG_MESSAGE_PREFIX + "Handling event {}", this, event);
        }
        catch (Exception e)
        {
            // serializing the event might fail (involves msgpack to JSON conversion)
            LOGGER.warn("Could not construct or write log message", e);
        }
    }

    protected void onUnhandledEventHandlingException(GeneralEventImpl event, Exception e)
    {
        throw new RuntimeException("Exception during handling of event " + event.getMetadata().getKey(), e);
    }

    public abstract String getTopicName();

    public int getPartitionId()
    {
        return partitionId;
    }

    protected abstract ActorFuture<Void> requestSubscriptionClose();
}
