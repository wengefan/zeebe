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
package io.zeebe.broker.transport;

import io.zeebe.broker.Loggers;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.BufferingServerTransport;
import io.zeebe.transport.Transports;
import io.zeebe.util.sched.ZbActorScheduler;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

public class BufferingServerTransportService implements Service<BufferingServerTransport>
{
    public static final Logger LOG = Loggers.TRANSPORT_LOGGER;

    protected final Injector<ZbActorScheduler> schedulerInjector = new Injector<>();
    protected final Injector<Dispatcher> receiveBufferInjector = new Injector<>();
    protected final Injector<Dispatcher> sendBufferInjector = new Injector<>();

    protected final String readableName;
    protected final InetSocketAddress bindAddress;

    protected BufferingServerTransport serverTransport;

    public BufferingServerTransportService(String readableName, InetSocketAddress bindAddress)
    {
        this.readableName = readableName;
        this.bindAddress = bindAddress;
    }

    @Override
    public void start(ServiceStartContext serviceContext)
    {
        final ZbActorScheduler scheduler = schedulerInjector.getValue();
        final Dispatcher receiveBuffer = receiveBufferInjector.getValue();
        final Dispatcher sendBuffer = sendBufferInjector.getValue();

        serverTransport = Transports.newServerTransport()
            .bindAddress(bindAddress)
            .sendBuffer(sendBuffer)
            .scheduler(scheduler)
            .buildBuffering(receiveBuffer);

        LOG.info("Bound {} to {}", readableName, bindAddress);
    }

    @Override
    public void stop(ServiceStopContext serviceStopContext)
    {
        serviceStopContext.async(serverTransport.closeAsync());
    }

    @Override
    public BufferingServerTransport get()
    {
        return serverTransport;
    }

    public Injector<Dispatcher> getReceiveBufferInjector()
    {
        return receiveBufferInjector;
    }

    public Injector<Dispatcher> getSendBufferInjector()
    {
        return sendBufferInjector;
    }

    public Injector<ZbActorScheduler> getSchedulerInjector()
    {
        return schedulerInjector;
    }

}
