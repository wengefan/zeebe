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
package io.zeebe.broker.clustering.management.service;

import io.zeebe.broker.clustering.management.ClusterManager;
import io.zeebe.broker.clustering.management.ClusterManagerContext;
import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.raft.Raft;
import io.zeebe.servicecontainer.*;
import io.zeebe.util.sched.ZbActorScheduler;

public class ClusterManagerService implements Service<ClusterManager>
{
    private final Injector<ClusterManagerContext> clusterManagementContextInjector = new Injector<>();
    private Injector<ZbActorScheduler> actorSchedulerInjector = new Injector<>();

    private ClusterManager clusterManager;
    private TransportComponentCfg config;
    private ServiceContainer serviceContainer;

    public ClusterManagerService(final ServiceContainer serviceContainer, final TransportComponentCfg config)
    {
        this.serviceContainer = serviceContainer;
        this.config = config;
    }

    private final ServiceGroupReference<Raft> raftGroupReference = ServiceGroupReference.<Raft>create()
            .onAdd((name, raft) -> clusterManager.addRaftCallback(name, raft))
            .onRemove((name, raft) -> clusterManager.removeRaftCallback(raft))
            .build();

    @Override
    public void start(ServiceStartContext startContext)
    {
        startContext.run(() ->
        {
            final ClusterManagerContext context = clusterManagementContextInjector.getValue();


            clusterManager = new ClusterManager(context, serviceContainer, config);

            actorSchedulerInjector.getValue().submitActor(clusterManager);
//            context.getActorScheduler().submitActor(clusterManager);
        });

    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        clusterManager.close();
    }

    @Override
    public ClusterManager get()
    {
        return clusterManager;
    }

    public Injector<ClusterManagerContext> getClusterManagementContextInjector()
    {
        return clusterManagementContextInjector;
    }


    public ServiceGroupReference<Raft> getRaftGroupReference()
    {
        return raftGroupReference;
    }

    public Injector<ZbActorScheduler> getActorSchedulerInjector()
    {
        return actorSchedulerInjector;
    }

}
