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
package io.zeebe.broker.clustering.gossip.service;

import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.gossip.Gossip;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.BufferingServerTransport;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.ZbActorScheduler;

public class GossipService implements Service<Gossip>
{
    private final Injector<ZbActorScheduler> actorSchedulerInjector = new Injector<>();
    private final Injector<ClientTransport> clientTransportInjector = new Injector<>();
    private final Injector<BufferingServerTransport> bufferingServerTransportInjector = new Injector<>();

    private Gossip gossip;
    private final TransportComponentCfg transportComponentCfg;

    public GossipService(TransportComponentCfg transportComponentCfg)
    {
        this.transportComponentCfg = transportComponentCfg;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        final ZbActorScheduler actorScheduler = actorSchedulerInjector.getValue();
        final SocketAddress host = new SocketAddress(transportComponentCfg.managementApi.getHost(transportComponentCfg.host), transportComponentCfg.managementApi.port);

        this.gossip = new Gossip(host, bufferingServerTransportInjector.getValue(),
                                 clientTransportInjector.getValue(), transportComponentCfg.gossip);


        actorScheduler.submitActor(gossip);
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(gossip.leave().onComplete((value, throwable) -> gossip.close()));
    }

    @Override
    public Gossip get()
    {
        return gossip;
    }

    public Injector<ZbActorScheduler> getActorSchedulerInjector()
    {
        return actorSchedulerInjector;
    }

    public Injector<ClientTransport> getClientTransportInjector()
    {
        return clientTransportInjector;
    }

    public Injector<BufferingServerTransport> getBufferingServerTransportInjector()
    {
        return bufferingServerTransportInjector;
    }
}
