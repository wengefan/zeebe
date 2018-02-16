package io.zeebe.client.task.impl.subscription;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.agrona.collections.IntArrayList;

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

public abstract class EventSubscriberGroup2
{

    protected final ActorControl actor;

    protected final List<EventSubscriber> subscribers = new CopyOnWriteArrayList<>();
    protected final ZeebeClientImpl client;

    protected final String topic;
    protected IntArrayList partitions;

    protected final EventAcquisition2 acquisition;

    protected CompletableActorFuture<EventSubscriberGroup2> openFuture;

    private volatile int state = STATE_OPENING;

    private static final int STATE_OPENING = 0;
    private static final int STATE_OPEN = 1;
    private static final int STATE_CLOSED = 2;

    public EventSubscriberGroup2(
            ActorControl actor,
            ZeebeClientImpl client,
            EventAcquisition2 acquisition,
            String topic)
    {
        this.actor = actor;
        this.acquisition = acquisition;
        this.client = client;
        this.topic = topic;
    }

    protected void open(CompletableActorFuture<EventSubscriberGroup2> openFuture)
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

                this.partitions = new IntArrayList();
                partitions.forEach(p -> this.partitions.add(p.getId()));

                // TODO: could also be a direct call
                actor.run(this::subscribeToPartitions);
            }
            else
            {
                // TODO: close wiht reason Topic %s is not known
                // TODO: closing exception should also contain the description of the group (=> see EventSubscriberGroup#describeGroupSpec)
            }
        });
    }

    public CompletableActorFuture<Void> closeAsync()
    {
        final CompletableActorFuture<Void> closeFuture = new CompletableActorFuture<>();

        // TODO: das ist nicht so toll; diese Klasse sollte selbst nicht actor.call aufrufen
        actor.call(() ->
        {
            subscribers.forEach(subscriber ->
            {
                final ActorFuture<Void> closeSubscriberFuture = subscriber.requestSubscriptionClose();

                actor.runOnCompletion(closeSubscriberFuture, (v, t) ->
                {
                    // TODO: what to do on exception?
                    onSubscriberClosed(subscriber);

                    if (subscribers.isEmpty())
                    {
                        closeFuture.complete(null);
                        state = STATE_CLOSED;
                    }
                });
            });
        });


        return closeFuture;
    }

    public void reopenSubscriptionsForRemoteAsync(RemoteAddress remoteAddress)
    {
        final Iterator<EventSubscriber> it = subscribers.iterator();

        while (it.hasNext())
        {
            final EventSubscriber subscriber = it.next();
            if (subscriber.getEventSource().equals(remoteAddress))
            {
                onSubscriberClosed(subscriber);
                // TODO: hier sollte man vll auch den Zustand des subscribers ändern
                openSubscriber(subscriber.getPartitionId());
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

    protected void subscribeToPartitions()
    {
        partitions.forEach(partition ->
        {
            // TODO: must tell the acquisition that we are opening a new subscriber
            //   as it may receive events for it before it has the subscriber in hand
            openSubscriber(partition);
        });
    }

    private void openSubscriber(int partitionId)
    {
        final ActorFuture<? extends EventSubscriptionCreationResult> future = requestNewSubscriber(partitionId);
        // TODO: must deal with the case when the #close-Command is received intermittently
        actor.runOnCompletion(future, (result, throwable) ->
        {
            if (throwable == null)
            {
                onSubscriberOpened(result);

                if (openFuture != null && hasSubscriberForEveryPartition())
                {
                    // TODO: this is fragile when subscribers are closing intermediately
                    openFuture.complete(this);
                    state = STATE_OPEN;
                }
            }
            else
            {
                // TODO: exception handling
            }
        });
    }

    protected boolean hasSubscriberForEveryPartition()
    {
        return partitions.size() == subscribers.size();
    }

    protected void onSubscriberOpened(EventSubscriptionCreationResult creationResult)
    {
        final EventSubscriber subscriber = buildSubscriber(creationResult);
        subscribers.add(subscriber);
        acquisition.addSubscriber(subscriber);
    }

    // TODO: Müssen die Invariante sicherstellen, dass für eine Gruppe gleichzeitig nur ein Subscriber
    //   pro Partition registriert ist
    protected void onSubscriberClosed(EventSubscriber subscriber)
    {
        acquisition.removeSubscriber(subscriber);
        subscribers.remove(subscriber);
    }

    public int pollEvents(CheckedConsumer<GeneralEventImpl> pollHandler)
    {
        int events = 0;
        for (EventSubscriber subscriber : subscribers)
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

}