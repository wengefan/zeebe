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
package io.zeebe.broker.clustering.raft;

import io.zeebe.broker.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftPersistentStorage;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.BufferingServerTransport;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.ZbActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

import java.util.List;

public class RaftService implements Service<Raft>
{
    private final SocketAddress socketAddress;
    private final LogStream logStream;
    private final List<SocketAddress> members;
    private final RaftPersistentStorage persistentStorage;
    private final RaftStateListener raftStateListener;
    private final LogStreamOpenActor logStreamOpenActor;
    private final LogStreamCloseActor logStreamCloseActor;

    private Injector<ZbActorScheduler> actorSchedulerInjector = new Injector<>();
    private Injector<BufferingServerTransport> serverTransportInjector = new Injector<>();
    private Injector<ClientTransport> clientTransportInjector = new Injector<>();
    private Raft raft;

    private CompletableActorFuture<Void> raftServiceCloseFuture;


    public RaftService(final SocketAddress socketAddress, final LogStream logStream, final List<SocketAddress> members, final RaftPersistentStorage persistentStorage, RaftStateListener raftStateListener)
    {
        this.socketAddress = socketAddress;
        this.logStream = logStream;
        this.members = members;
        this.persistentStorage = persistentStorage;
        this.raftStateListener = raftStateListener;
        this.logStreamOpenActor = new LogStreamOpenActor();
        this.logStreamCloseActor = new LogStreamCloseActor();
    }

    private CompletableActorFuture<Void> raftServiceOpenFuture;
    @Override
    public void start(final ServiceStartContext startContext)
    {
        raftServiceOpenFuture = new CompletableActorFuture<>();
        startContext.getScheduler().submitActor(logStreamOpenActor);

        startContext.async(raftServiceOpenFuture);
    }

    @Override
    public void stop(final ServiceStopContext stopContext)
    {
        raftServiceCloseFuture = new CompletableActorFuture<>();
        actorSchedulerInjector.getValue().submitActor(logStreamCloseActor);

        stopContext.async(raftServiceCloseFuture);
    }

    @Override
    public Raft get()
    {
        return raft;
    }

    public Injector<ZbActorScheduler> getActorSchedulerInjector()
    {
        return actorSchedulerInjector;
    }

    public Injector<BufferingServerTransport> getServerTransportInjector()
    {
        return serverTransportInjector;
    }

    public Injector<ClientTransport> getClientTransportInjector()
    {
        return clientTransportInjector;
    }

    private class LogStreamOpenActor extends ZbActor
    {
        @Override
        protected void onActorStarted()
        {
            final ActorFuture<Void> logStreamOpen = logStream.openAsync();
            actor.runOnCompletion(logStreamOpen, ((aVoid, throwable) ->
            {
                if (throwable == null)
                {
                    final BufferingServerTransport serverTransport = serverTransportInjector.getValue();
                    final ClientTransport clientTransport = clientTransportInjector.getValue();
                    raft = new Raft(socketAddress, logStream, serverTransport, clientTransport, persistentStorage);
                    raft.registerRaftStateListener(raftStateListener);

                    raft.addMembers(members);
                    // TODO submit raft
                    final ZbActorScheduler actorScheduler = actorSchedulerInjector.getValue();
//                actorScheduler.submitActor(raft);
                }
                else
                {
                    Loggers.CLUSTERING_LOGGER.debug("Failed to open log stream.");
                }
                raftServiceOpenFuture.complete(null);
            }));
        }
    }

    private class LogStreamCloseActor extends ZbActor
    {
        @Override
        protected void onActorStarted()
        {
            raft.close();
            final ActorFuture<Void> closeLogStreamController = logStream.closeLogStreamController();
            actor.runOnCompletion(closeLogStreamController, ((aVoid, throwable) ->
            {
                final ActorFuture<Void> closeLogstream = logStream.closeAsync();
                actor.runOnCompletion(closeLogstream, ((aVoid1, throwable1) ->
                {
                    raftServiceCloseFuture.complete(null);
                }));
            }));
        }
    }
}
