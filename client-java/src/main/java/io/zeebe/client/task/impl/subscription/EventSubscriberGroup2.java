package io.zeebe.client.task.impl.subscription;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2ObjectHashMap;

import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.client.topic.Partition;
import io.zeebe.client.topic.Topic;
import io.zeebe.client.topic.Topics;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public abstract class EventSubscriberGroup2
{

    protected final ActorControl actor;

    protected final Long2ObjectHashMap<EventSubscriber> subscribers = new Long2ObjectHashMap<>();
    protected final ZeebeClientImpl client;

    protected final String topic;
    protected IntArrayList partitions;

    protected final EventAcquisition2 acquisition;

    protected CompletableActorFuture<EventSubscriberGroup2> openFuture;

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
            }
        });
    }

    public CompletableActorFuture<EventSubscriberGroup2> closeAsync()
    {
        final CompletableActorFuture<EventSubscriberGroup2> closeFuture = new CompletableActorFuture<>();

        // TODO: das ist nicht so toll; diese Klasse sollte selbst nicht actor.call aufrufen
        actor.call(() ->
        {
            subscribers.values().forEach(subscriber ->
            {
                final ActorFuture<Void> closeSubscriberFuture =
                        requestCloseSubscriber(subscriber.getPartitionId(), subscriber.getSubscriberKey());

                actor.runOnCompletion(closeSubscriberFuture, (v, t) ->
                {
                    // TODO: what to do on exception?
                    onSubscriberClosed(subscriber);

                    if (subscribers.isEmpty())
                    {
                        closeFuture.complete(this);
                    }
                });
            });
        });


        return closeFuture;
    }

    public void reopenSubscriptionsForRemoteAsync(RemoteAddress remoteAddress)
    {
        final Iterator<EventSubscriber> it = subscribers.values().iterator();

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
        final ActorFuture<EventSubscriptionCreationResult> future = requestNewSubscriber(partitionId);
        // TODO: must deal with the case when the #close-Command is received intermittently
        actor.runOnCompletion(future, (result, throwable) ->
        {
            if (throwable == null)
            {
                onSubscriberOpened(result);

                if (openFuture != null && hasSubscriberForEveryPartition())
                {
                    openFuture.complete(this);
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
        subscribers.put(subscriber.getSubscriberKey(), subscriber);
        acquisition.addSubscriber(subscriber);
    }

    // TODO: Müssen die Invariante sicherstellen, dass für eine Gruppe gleichzeitig nur ein Subscriber
    //   pro Partition registriert ist
    protected void onSubscriberClosed(EventSubscriber subscriber)
    {
        acquisition.removeSubscriber(subscriber);
        subscribers.remove(subscriber.getSubscriberKey());
    }

    protected abstract ActorFuture<EventSubscriptionCreationResult> requestNewSubscriber(int partitionId);

    protected abstract ActorFuture<Void> requestCloseSubscriber(int partitionId, long subscriberKey);

    protected abstract EventSubscriber buildSubscriber(EventSubscriptionCreationResult result);

    public abstract boolean isManagedGroup();
}