/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering.management;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.LogStreamService;
import io.zeebe.broker.logstreams.LogStreamServiceNames;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.Protocol;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ZbActor;
import org.agrona.DirectBuffer;

import java.util.concurrent.CompletableFuture;

import static io.zeebe.broker.clustering.ClusterServiceNames.CLUSTER_MANAGER_SERVICE;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.logStreamServiceName;
import static io.zeebe.broker.system.SystemServiceNames.ACTOR_SCHEDULER_SERVICE;

public class StartLogStreamServiceController extends ZbActor
{
    private final OnOpenLogStreamListener onOpenCallback;
    private final Raft raft;
    private final ServiceName<Raft> raftServiceName;
    private final ServiceContainer serviceContainer;
    private final ServiceName<LogStream> serviceName;

    // listeners
    private final OnFollowerListener onFollowerListener;
    private final OnLeaderListener onLeaderListener;

    public StartLogStreamServiceController(final ServiceName<Raft> raftServiceName, final Raft raft, final ServiceContainer serviceContainer, OnOpenLogStreamListener callable)
    {
        this.onOpenCallback = callable;
        this.raftServiceName = raftServiceName;
        this.raft = raft;
        this.serviceContainer = serviceContainer;
        this.serviceName = logStreamServiceName(raft.getLogStream().getLogName());
        this.onFollowerListener = new OnFollowerListener();
        this.onLeaderListener = new OnLeaderListener();
    }

    @Override
    protected void onActorStarted()
    {
        raft.registerRaftStateListener(onLeaderListener);
        actor.onCondition("alive-start-logstream-ctrl", () -> { });
    }

    private void startLogStream()
    {

        Loggers.CLUSTERING_LOGGER.debug("Start log stream...topic {}", BufferUtil.bufferAsString(raft.getLogStream().getTopicName()));
        final LogStream logStream = raft.getLogStream();
        final LogStreamService service = new LogStreamService(logStream);

        final ServiceName<LogStream> streamGroup = Protocol.SYSTEM_TOPIC_BUF.equals(logStream.getTopicName()) ?
            LogStreamServiceNames.SYSTEM_STREAM_GROUP :
            LogStreamServiceNames.WORKFLOW_STREAM_GROUP;

        final CompletableFuture<Void> future =
            serviceContainer
                .createService(serviceName, service)
                .dependency(ACTOR_SCHEDULER_SERVICE)
                .dependency(CLUSTER_MANAGER_SERVICE)
                .dependency(raftServiceName)
                .group(streamGroup)
                .install();

        future.whenComplete((v, throwable) ->
        {
            actor.call(() ->
            {
                onOpenCallback.onOpenLogStreamService(raft.getLogStream());

                // remove follower listener
                raft.removeRaftStateListener(onFollowerListener);

                raft.registerRaftStateListener(onFollowerListener);
            });
        });
    }

    private class OnFollowerListener implements RaftStateListener
    {
        @Override
        public void onStateChange(int i, DirectBuffer directBuffer, SocketAddress socketAddress, RaftState raftState)
        {
            if (raftState == RaftState.FOLLOWER)
            {
                actor.call(() ->
                {

                    if (serviceContainer.hasService(serviceName))
                    {
                        final CompletableFuture<Void> future = serviceContainer.removeService(serviceName);

                        future.whenComplete((aVoid, throwable) ->
                        {
                            actor.call(() ->
                            {
                                // remove follower listener
                                raft.removeRaftStateListener(onFollowerListener);

                                // add leader listener
                                raft.registerRaftStateListener(onLeaderListener);
                            });
                        });
                    }
                });
            }
        }
    }

    private class OnLeaderListener implements RaftStateListener
    {
        @Override
        public void onStateChange(int i, DirectBuffer directBuffer, SocketAddress socketAddress, RaftState raftState)
        {
            if (raftState == RaftState.LEADER)
            {
                actor.call(() -> startLogStream());
            }
        }
    }

    @Override
    protected void onActorClosing()
    {
        raft.removeRaftStateListener(onFollowerListener);
        raft.removeRaftStateListener(onLeaderListener);
    }

    public Raft getRaft()
    {
        return raft;
    }

    public void close()
    {
        actor.close();
    }
}
