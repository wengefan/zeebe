package io.zeebe.client.task.impl.subscription;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.agrona.collections.Int2ObjectHashMap;

import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.event.impl.GeneralEventImpl;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.client.topic.Partition;
import io.zeebe.client.topic.Topic;
import io.zeebe.client.topic.Topics;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.CheckedConsumer;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public abstract class EventSubscriberGroup
{

    protected final ActorControl actor;

    protected final ZeebeClientImpl client;
    protected final Int2ObjectHashMap<SubscriberState> subscriberState = new Int2ObjectHashMap<>();

    // thread-safe data structure for iteration by subscription executors from another thread
    protected final List<EventSubscriber> subscribersList = new CopyOnWriteArrayList<>();

    protected final String topic;
    protected final SubscriptionManager acquisition;

    protected CompletableActorFuture<EventSubscriberGroup> openFuture;
    protected List<CompletableActorFuture<Void>> closeFutures = new ArrayList<>();

    private volatile int state = STATE_OPENING;

    private static final int STATE_OPENING = 0;
    private static final int STATE_OPEN = 1;
    private static final int STATE_CLOSING = 2;
    private static final int STATE_CLOSED = 3;

    public EventSubscriberGroup(
            ActorControl actor,
            ZeebeClientImpl client,
            SubscriptionManager acquisition,
            String topic)
    {
        this.actor = actor;
        this.acquisition = acquisition;
        this.client = client;
        this.topic = topic;
    }

    protected void open(CompletableActorFuture<EventSubscriberGroup> openFuture)
    {
        this.openFuture = openFuture;

        final ActorFuture<Topics> topicsFuture = client.topics().getTopics().executeAsync();
        actor.runOnCompletion(topicsFuture, (topics, failure) ->
        {
            // TODO: handle failure
            final Optional<Topic> requestedTopic =
                topics.getTopics()
                    .stream()
                    .filter(t -> topic.equals(t.getName()))
                    .findFirst();

            if (requestedTopic.isPresent())
            {
                final List<Partition> partitions = requestedTopic.get().getPartitions();

                partitions.forEach(p -> openSubscriber(p.getId()));
            }
            else
            {
                // TODO: close wiht reason Topic %s is not known
                // TODO: closing exception should also contain the description of the group (=> see EventSubscriberGroup#describeGroupSpec)
            }
        });
    }


    public void doClose(final CompletableActorFuture<Void> closeFuture)
    {
        if (state == STATE_OPENING || state == STATE_CLOSING)
        {
            this.closeFutures.add(closeFuture);
        }
        else if (state == STATE_CLOSED)
        {
            closeFuture.complete(null);
        }
        else if (state == STATE_OPEN)
        {
            this.closeFutures.add(closeFuture);
            state = STATE_CLOSING;

            subscribersList.forEach(subscriber -> closeSubscriber(subscriber));
        }
    }

    private void onGroupClosed()
    {
        if (openFuture != null)
        {
            // TODO: proper context-based exception and message
            openFuture.completeExceptionally(new RuntimeException("could not open subscriber group"));
            openFuture = null;
        }

        closeFutures.forEach(f -> f.complete(null));
        closeFutures.clear();
    }

    private void onGroupOpened()
    {
        if (openFuture != null)
        {
            openFuture.complete(this);
            openFuture = null;
        }

        if (!closeFutures.isEmpty())
        {
            doClose(null);
        }
    }

    public ActorFuture<Void> closeAsync()
    {

        return acquisition.closeGroup(this);
    }

    public void reopenSubscriptionsForRemoteAsync(RemoteAddress remoteAddress)
    {
        final Iterator<EventSubscriber> it = subscribersList.iterator();

        while (it.hasNext())
        {
            final EventSubscriber subscriber = it.next();
            if (subscriber.getEventSource().equals(remoteAddress))
            {
                subscriber.disable();
                onSubscriberClosed(subscriber);

                if (state == STATE_OPEN)
                {
                    openSubscriber(subscriber.getPartitionId());
                }
            }
        }
    }

    public void close()
    {
        try
        {
            closeAsync().get();
        }
        catch (Exception e)
        {
            throw new ClientException("Exception while closing subscription", e);
        }
    }

    private void openSubscriber(int partitionId)
    {
        // TODO: must tell the acquisition that we are opening a new subscriber
        //   as it may receive events for it before it has the subscriber in hand

        System.out.println("Opening subscriber to partition " + partitionId);

        this.subscriberState.put(partitionId, SubscriberState.SUBSCRIBING);
        final ActorFuture<? extends EventSubscriptionCreationResult> future = requestNewSubscriber(partitionId);
        // TODO: must deal with the case when the #close-Command is received intermittently
        actor.runOnCompletion(future, (result, throwable) ->
        {
            if (throwable == null)
            {
                onSubscriberOpened(result);
            }
            else
            {
                onSubscriberOpenFailed(partitionId, throwable);
            }
        });
    }

    private void closeSubscriber(EventSubscriber subscriber)
    {
        subscriber.disable();
        subscriberState.put(subscriber.getPartitionId(), SubscriberState.UNSUBSCRIBING);

        actor.runUntilDone(() ->
        {
            if (!subscriber.hasEventsInProcessing())
            {
                final ActorFuture<Void> closeSubscriberFuture = subscriber.requestSubscriptionClose();
                actor.runOnCompletion(closeSubscriberFuture, (v, t) ->
                {
                    // TODO: what to do on exception?
                    onSubscriberClosed(subscriber);
                });
                actor.done();
            }
            else
            {
                actor.yield();
            }
        });
    }


    private void onSubscriberOpenFailed(int partitionId, Throwable t)
    {
        // TODO: exception handling
        System.out.println("Opening subscriber failed; Closing group");
        subscriberState.put(partitionId, SubscriberState.NOT_SUBSCRIBED);

        if (!checkGroupClosed())
        {
            doClose(null);
        }
    }

    private void onSubscriberOpened(EventSubscriptionCreationResult result)
    {
        System.out.println("Subscriber opened successfully");

        final EventSubscriber subscriber = buildSubscriber(result);
        subscriberState.put(subscriber.getPartitionId(), SubscriberState.SUBSCRIBED);

        subscribersList.add(subscriber);
        acquisition.addSubscriber(subscriber);

        if (state == STATE_OPENING && allPartitionsSubscribed())
        {
            state = STATE_OPEN;
            onGroupOpened();
        }
        else if (state == STATE_CLOSING)
        {
            closeSubscriber(subscriber);
        }
    }

    private void onSubscriberClosed(EventSubscriber subscriber)
    {
        acquisition.removeSubscriber(subscriber);
        subscriberState.put(subscriber.getPartitionId(), SubscriberState.NOT_SUBSCRIBED);
        subscribersList.add(subscriber);
        acquisition.removeSubscriber(subscriber);

        checkGroupClosed();
    }

    private boolean checkGroupClosed()
    {
        if (state == STATE_CLOSING && allPartitionsNotSubscribed())
        {
            state = STATE_CLOSED;
            onGroupClosed();
            return true;
        }
        else
        {
            return false;
        }
    }

    private boolean allPartitionsSubscribed()
    {
        return allPartitionsInSubscriberState(SubscriberState.SUBSCRIBED);
    }

    private boolean allPartitionsNotSubscribed()
    {
        return allPartitionsInSubscriberState(SubscriberState.NOT_SUBSCRIBED);
    }

    private boolean allPartitionsInSubscriberState(SubscriberState state)
    {
        return subscriberState.values().stream().allMatch(s -> s == state);
    }


    public int pollEvents(CheckedConsumer<GeneralEventImpl> pollHandler)
    {
        int events = 0;
        for (EventSubscriber subscriber : subscribersList)
        {
            events += subscriber.pollEvents(pollHandler);
        }

        return events;
    }

    public boolean isOpen()
    {
        return state == STATE_OPEN;
    }

    public boolean isClosed()
    {
        return state == STATE_CLOSED;
    }

    public abstract int poll();

    protected abstract ActorFuture<? extends EventSubscriptionCreationResult> requestNewSubscriber(int partitionId);

    protected abstract EventSubscriber buildSubscriber(EventSubscriptionCreationResult result);

    public abstract boolean isManagedGroup();

    enum SubscriberState
    {
        NOT_SUBSCRIBED, UNSUBSCRIBING, SUBSCRIBING, SUBSCRIBED;
    }

}