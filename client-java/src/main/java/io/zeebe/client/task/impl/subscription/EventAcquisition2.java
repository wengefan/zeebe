package io.zeebe.client.task.impl.subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;

import io.zeebe.client.event.EventMetadata;
import io.zeebe.client.event.impl.GeneralEventImpl;
import io.zeebe.client.event.impl.TopicSubscriberGroup;
import io.zeebe.client.event.impl.TopicSubscriptionSpec;
import io.zeebe.client.impl.Loggers;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.transport.ClientInputMessageSubscription;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

// TODO: consider renaming to subscriptionmanager
public class EventAcquisition2 extends ZbActor implements SubscribedEventHandler
{
    protected static final Logger LOGGER = Loggers.SUBSCRIPTION_LOGGER;

    protected final ZeebeClientImpl client;

    private ClientInputMessageSubscription messageSubscription;
    private final EventSubscribers taskSubscribers = new EventSubscribers();
    private final EventSubscribers topicSubscribers = new EventSubscribers();

    final IdleStrategy idleStrategy = new BackoffIdleStrategy(1000, 100, 1, TimeUnit.MILLISECONDS.toNanos(1));
    final ErrorHandler errorHandler = Throwable::printStackTrace;

    private final List<AgentRunner> agentRunners = new ArrayList<>();

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

        startSubscriptionExecution(client.getNumExecutionThreads());
    }

    private void startSubscriptionExecution(int numThreads)
    {
        for (int i = 0; i < numThreads; i++)
        {
            final SubscriptionExecutor executor = new SubscriptionExecutor(topicSubscribers);
            final AgentRunner agentRunner = initAgentRunner(executor);
            AgentRunner.startOnThread(agentRunner);

            agentRunners.add(agentRunner);
        }
    }

    private void stopSubscriptionExecution()
    {
        for (AgentRunner runner: agentRunners)
        {
            runner.close();
        }
    }

    private AgentRunner initAgentRunner(Agent agent)
    {
        return new AgentRunner(idleStrategy, errorHandler, null, agent);
    }

    @Override
    protected void onActorClosing()
    {
        closeAllSubscribers();

        // TODO: das hier blockiert jetzt im Kontext des ActorSchedulers; könnte aber ok sein (oder sonst als pollBlocking abgeben)
        stopSubscriptionExecution();
    }

    public ActorFuture<EventSubscriberGroup2> openTopicSubscription(TopicSubscriptionSpec spec)
    {
        final CompletableActorFuture<EventSubscriberGroup2> future = new CompletableActorFuture<>();
        actor.call(() ->
        {
            final EventSubscriberGroup2 group = new TopicSubscriberGroup(actor, client, this, spec);
            topicSubscribers.addGroup(group);
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
        throw new RuntimeException("not yet implemented");
//        actor.call(() ->
//        {
//
//        });

//        asyncContext.runAsync(() -> subscribers.reopenSubscribersForRemote(remoteAddress));
    }

    public ActorFuture<Void> closeGroup(EventSubscriberGroup2 group)
    {
        return group.closeAsync();
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

    public ActorFuture<Void> close()
    {
        return actor.close();
    }

}