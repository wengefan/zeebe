package io.zeebe.client.task.impl.subscription;

import java.util.List;

import org.slf4j.Logger;

import io.zeebe.client.event.EventMetadata;
import io.zeebe.client.event.impl.GeneralEventImpl;
import io.zeebe.client.event.impl.TopicSubscriptionSpec;
import io.zeebe.client.impl.Loggers;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.client.topic.Partition;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.transport.ClientInputMessageSubscription;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class EventAcquisition2 extends ZbActor implements SubscribedEventHandler
{
    protected static final Logger LOGGER = Loggers.SUBSCRIPTION_LOGGER;

    protected final ZeebeClientImpl client;

    private ClientInputMessageSubscription messageSubscription;
    private final EventSubscribers taskSubscribers = new EventSubscribers();
    private final EventSubscribers topicSubscribers = new EventSubscribers();

    public EventAcquisition2(ZeebeClientImpl client)
    {
        this.client = client;
    }

    @Override
    protected void onActorStarted()
    {
        final SubscribedEventCollector taskCollector = new SubscribedEventCollector(
                this,
                client.getMsgPackConverter());

        actor.await(client.getTransport().openSubscription("event-acquisition", taskCollector),
            (s, t) ->
            {
                this.messageSubscription = s;
                actor.consume(s, this::pollInput);
            });
    }

    public ActorFuture<EventSubscriberGroup2> openTopicSubscription(TopicSubscriptionSpec spec)
    {
        CompletableActorFuture<EventSubscriberGroup2> future = new CompletableActorFuture<>();
        actor.call(() ->
        {
            EventSubscriberGroup2 group = null; // TODO: instantiate actual group here
            group.open(future);
        });

        return future;
    }

    protected void pollInput()
    {
        // TODO: muss man hier noch mehr machen?
        messageSubscription.poll();
    }

    public void addSubscriber(EventSubscriber subscriber)
    {
        // TODO: distinguish task and topic subscribers
        topicSubscribers.add(subscriber);

    }

    public void removeSubscriber(EventSubscriber subscriber)
    {
        topicSubscribers.remove(subscriber);
    }

    public void closeAllSubscribers()
    {
        topicSubscribers.closeAllGroups();
    }

    public ActorFuture<Void> reopenSubscriptionsForRemoteAsync(RemoteAddress remoteAddress)
    {
        actor.call(() ->
        {

        });

        asyncContext.runAsync(() -> subscribers.reopenSubscribersForRemote(remoteAddress));
    }

    @Override
    public boolean onEvent(SubscriptionType type, long subscriberKey, GeneralEventImpl event)
    {
        final EventMetadata eventMetadata = event.getMetadata();
        // TODO: make work with task subscribers

        EventSubscriber subscriber = topicSubscribers.getSubscriber(eventMetadata.getPartitionId(), subscriberKey);

        if (subscriber == null)
        {
            // TODO: restore this logic
//            if (subscribers.isAnySubscriberOpening())
//            {
//                // avoids a race condition when a subscribe request is in progress and we haven't activated the subscriber
//                // yet, but we already receive an event from the broker
//                // in this case, we postpone the event
//                return false;
//            }
//            else
//            {
                // fetch a second time as the subscriber may have opened (and registered) between the first #getSubscriptions
                // invocation and the check for opening subscribers
                subscriber = topicSubscribers.getSubscriber(eventMetadata.getPartitionId(), subscriberKey);
//            }
        }

        if (subscriber != null && subscriber.isOpen())
        {
            event.setTopicName(subscriber.getTopicName());
            return subscriber.addEvent(event);
        }
        else
        {
            LOGGER.debug("Event Acquisition: Ignoring event " + event.toString() + " for subscription " + subscriberKey);
            return true; // ignoring the event is success; don't want to retry it later
        }
    }

}
