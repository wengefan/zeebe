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
import io.zeebe.client.event.impl.TopicSubscriber;
import io.zeebe.client.event.impl.TopicSubscriberGroup;
import io.zeebe.client.event.impl.TopicSubscriptionSpec;
import io.zeebe.client.impl.Loggers;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.transport.ClientInputMessageSubscription;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.TransportListener;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class SubscriptionManager extends ZbActor implements SubscribedEventHandler, TransportListener
{
    protected static final Logger LOGGER = Loggers.SUBSCRIPTION_LOGGER;

    protected final ZeebeClientImpl client;

    private ClientInputMessageSubscription messageSubscription;
    private final EventSubscribers taskSubscribers = new EventSubscribers();
    private final EventSubscribers topicSubscribers = new EventSubscribers();

    final IdleStrategy idleStrategy = new BackoffIdleStrategy(1000, 100, 1, TimeUnit.MILLISECONDS.toNanos(1));
    final ErrorHandler errorHandler = Throwable::printStackTrace;

    private final List<AgentRunner> agentRunners = new ArrayList<>();

    public SubscriptionManager(ZeebeClientImpl client)
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

        // TODO: <hack> prevent autoclose
        actor.onCondition("foo", () ->
        {
        });
    }

    private void startSubscriptionExecution(int numThreads)
    {
        for (int i = 0; i < numThreads; i++)
        {
            final SubscriptionExecutor executor = new SubscriptionExecutor(topicSubscribers, taskSubscribers);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ActorFuture<TopicSubscriberGroup> openTopicSubscription(TopicSubscriptionSpec spec)
    {
        final CompletableActorFuture<TopicSubscriberGroup> future = new CompletableActorFuture<>();
        actor.call(() ->
        {
            final TopicSubscriberGroup group = new TopicSubscriberGroup(actor, client, this, spec);
            topicSubscribers.addGroup(group);
            group.open((CompletableActorFuture) future);
        });

        return future;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ActorFuture<TaskSubscriberGroup> openTaskSubscription(TaskSubscriptionSpec spec)
    {
        final CompletableActorFuture<TaskSubscriberGroup> future = new CompletableActorFuture<>();
        actor.call(() ->
        {
            final TaskSubscriberGroup group = new TaskSubscriberGroup(actor, client, this, spec);
            taskSubscribers.addGroup(group);
            group.open((CompletableActorFuture) future);
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
        if (subscriber instanceof TopicSubscriber)
        {
            topicSubscribers.add(subscriber);
        }
        else
        {
            taskSubscribers.add(subscriber);
        }

    }

    public void removeSubscriber(EventSubscriber subscriber)
    {
        if (subscriber instanceof TopicSubscriber)
        {
            topicSubscribers.remove(subscriber);
        }
        else
        {
            taskSubscribers.remove(subscriber);
        }
    }

    public void closeAllSubscribers()
    {
        topicSubscribers.closeAllGroups();
        taskSubscribers.closeAllGroups();
    }

    public ActorFuture<Void> reopenSubscriptionsForRemoteAsync(RemoteAddress remoteAddress)
    {
        return actor.call(() ->
        {
            topicSubscribers.reopenSubscribersForRemote(remoteAddress);
            taskSubscribers.reopenSubscribersForRemote(remoteAddress);
        });
    }

    public ActorFuture<Void> closeGroup(EventSubscriberGroup<?> group)
    {
        final CompletableActorFuture<Void> closeFuture = new CompletableActorFuture<>();
        actor.call(() -> group.doClose(closeFuture));
        return closeFuture;
    }

    @Override
    public boolean onEvent(SubscriptionType type, long subscriberKey, GeneralEventImpl event)
    {
        final EventMetadata eventMetadata = event.getMetadata();
        // TODO: make work with task subscribers

        final EventSubscribers subscribers;

        if (type == SubscriptionType.TASK_SUBSCRIPTION)
        {
            subscribers = taskSubscribers;
        }
        else if (type == SubscriptionType.TOPIC_SUBSCRIPTION)
        {
            subscribers = topicSubscribers;
        }
        else
        {
            subscribers = null;
        }

        EventSubscriber subscriber = null;

        if (subscribers != null)
        {
            subscriber = subscribers.getSubscriber(eventMetadata.getPartitionId(), subscriberKey);

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
        }

        if (subscriber != null && subscriber.isOpen())
        {
            event.setTopicName(subscriber.getTopicName());
            return subscriber.addEvent(event);
        }
        else
        {
            LOGGER.debug("Ignoring event event %s for subscription [type=%s, partition=%s, key=%s]",
                    event, type, event.getMetadata().getPartitionId(), subscriberKey);
            return true; // ignoring the event is success; don't want to retry it later
        }
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }


    @Override
    public void onConnectionEstablished(RemoteAddress remoteAddress)
    {
    }


    @Override
    public void onConnectionClosed(RemoteAddress remoteAddress)
    {
        reopenSubscriptionsForRemoteAsync(remoteAddress);
    }

}
