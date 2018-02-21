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
package io.zeebe.broker.system.deployment.service;

import io.zeebe.broker.clustering.management.PartitionManager;
import io.zeebe.broker.logstreams.LogStreamServiceNames;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.SystemConfiguration;
import io.zeebe.broker.system.SystemServiceNames;
import io.zeebe.broker.system.deployment.PendingDeploymentCheck;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.TopicPartitions;
import io.zeebe.broker.system.deployment.data.WorkflowVersions;
import io.zeebe.broker.system.deployment.handler.WorkflowRequestMessageSender;
import io.zeebe.broker.system.deployment.processor.*;
import io.zeebe.broker.workflow.data.DeploymentState;
import io.zeebe.broker.workflow.data.WorkflowState;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;

import java.time.Duration;

public class DeploymentManager implements Service<DeploymentManager>
{
    private final Injector<PartitionManager> partitionManagerInjector = new Injector<>();
    private final Injector<ClientTransport> managementClientInjector = new Injector<>();
    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();

    private final SystemConfiguration systemConfiguration;

    private ServiceStartContext serviceContext;

    private PartitionManager partitionManager;
    private ClientTransport managementClient;
    private ServerTransport clientApiTransport;
    private PendingDeploymentCheck pendingDeploymentCheck;

    private final ServiceGroupReference<LogStream> systemStreamGroupReference = ServiceGroupReference.<LogStream>create()
            .onAdd((name, stream) -> installDeploymentStreamProcessor(stream, name))
            .build();

    public DeploymentManager(SystemConfiguration systemConfiguration)
    {
        this.systemConfiguration = systemConfiguration;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        serviceContext = startContext;

        partitionManager = partitionManagerInjector.getValue();
        managementClient = managementClientInjector.getValue();
        clientApiTransport = clientApiTransportInjector.getValue();
    }

    private void installDeploymentStreamProcessor(final LogStream logStream, ServiceName<LogStream> serviceName)
    {

        final WorkflowVersions workflowVersions = new WorkflowVersions();
        final PendingDeployments pendingDeployments = new PendingDeployments();
        final PendingWorkflows pendingWorkflows = new PendingWorkflows();

        final WorkflowRequestMessageSender workflowRequestMessageSender = new WorkflowRequestMessageSender(partitionManager, managementClient);

        final Duration deploymentRequestTimeout = Duration.ofSeconds(systemConfiguration.getDeploymentCreationTimeoutSeconds());

        final TypedStreamEnvironment streamEnvironment = new TypedStreamEnvironment(logStream, clientApiTransport.getOutput());

        final TypedEventStreamProcessorBuilder streamProcessorBuilder = streamEnvironment.newStreamProcessor();

        final PartitionCollector partitionCollector = new PartitionCollector();
        partitionCollector.registerWith(streamProcessorBuilder);
        final TopicPartitions partitions = partitionCollector.getPartitions();

        pendingDeploymentCheck = new PendingDeploymentCheck(
            workflowRequestMessageSender,
            streamEnvironment.buildStreamReader(),
            streamEnvironment.buildStreamWriter(),
            pendingDeployments,
            pendingWorkflows);

        final TypedStreamProcessor streamProcessor = streamProcessorBuilder
            .onEvent(EventType.DEPLOYMENT_EVENT, DeploymentState.CREATE, new DeploymentCreateProcessor(partitions, workflowVersions, pendingDeployments, deploymentRequestTimeout))
            .onEvent(EventType.DEPLOYMENT_EVENT, DeploymentState.VALIDATED, new DeploymentValidatedProcessor(pendingDeployments))
            .onEvent(EventType.WORKFLOW_EVENT, WorkflowState.CREATE, new WorkflowCreateProcessor(partitions, pendingDeployments, pendingWorkflows, workflowRequestMessageSender))
            .onEvent(EventType.DEPLOYMENT_EVENT, DeploymentState.DISTRIBUTED, new DeploymentDistributedProcessor(pendingDeployments, pendingWorkflows))
            .onEvent(EventType.DEPLOYMENT_EVENT, DeploymentState.TIMED_OUT, new DeploymentTimedOutProcessor(pendingDeployments, pendingWorkflows, streamEnvironment.buildStreamReader()))
            .onEvent(EventType.WORKFLOW_EVENT, WorkflowState.DELETE, new WorkflowDeleteProcessor(pendingDeployments, pendingWorkflows, workflowVersions, workflowRequestMessageSender))
            .onEvent(EventType.DEPLOYMENT_EVENT, DeploymentState.REJECT, new DeploymentRejectProcessor(pendingDeployments))
            .withStateResource(workflowVersions.getRawMap())
            .withStateResource(pendingDeployments.getRawMap())
            .withStateResource(pendingWorkflows.getRawMap())
            .withListener(new StreamProcessorLifecycleAware()
            {
                @Override
                public void onOpen(TypedStreamProcessor streamProcessor)
                {
                    // TODO check needs to be re-implemented
                    streamProcessor.getActor().runAtFixedRate(Duration.ofMillis(250), pendingDeploymentCheck);
                }
            })
            .build();

        final StreamProcessorService streamProcessorService = new StreamProcessorService(
             "deployment",
             StreamProcessorIds.DEPLOYMENT_PROCESSOR_ID,
             streamProcessor)
             .eventFilter(streamProcessor.buildTypeFilter());

        serviceContext.createService(SystemServiceNames.DEPLOYMENT_PROCESSOR, streamProcessorService)
             .dependency(serviceName, streamProcessorService.getLogStreamInjector())
             .dependency(LogStreamServiceNames.SNAPSHOT_STORAGE_SERVICE, streamProcessorService.getSnapshotStorageInjector())
             .dependency(SystemServiceNames.ACTOR_SCHEDULER_SERVICE, streamProcessorService.getActorSchedulerInjector())
             .install();
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        if (pendingDeploymentCheck != null)
        {
            pendingDeploymentCheck.close();
        }
    }

    @Override
    public DeploymentManager get()
    {
        return this;
    }

    public Injector<PartitionManager> getPartitionManagerInjector()
    {
        return partitionManagerInjector;
    }

    public Injector<ClientTransport> getManagementClientInjector()
    {
        return managementClientInjector;
    }
    public ServiceGroupReference<LogStream> getSystemStreamGroupReference()
    {
        return systemStreamGroupReference;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }

}
