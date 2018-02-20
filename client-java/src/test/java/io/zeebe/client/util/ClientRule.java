/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.util;

import java.util.Properties;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import io.zeebe.client.TasksClient;
import io.zeebe.client.TopicsClient;
import io.zeebe.client.WorkflowsClient;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import io.zeebe.util.sched.clock.ControlledActorClock;

public class ClientRule extends ExternalResource
{

    protected final Properties properties;

    protected ZeebeClient client;
    protected ControlledActorClock clock;

    public ClientRule()
    {
        this(() -> new Properties());
    }

    public ClientRule(Supplier<Properties> propertiesProvider)
    {
        this.properties = propertiesProvider.get();

    }

    @Override
    protected void before() throws Throwable
    {
        clock = new ControlledActorClock();
        client = new ZeebeClientImpl(properties, clock);
    }

    public ControlledActorClock getClock()
    {
        return clock;
    }

    @Override
    protected void after()
    {
        clock.reset();
        client.close();
    }

    public ZeebeClient getClient()
    {
        return client;
    }

    public TopicsClient topics()
    {
        return client.topics();
    }

    public WorkflowsClient workflows()
    {
        return client.workflows();
    }

    public TasksClient tasks()
    {
        return client.tasks();
    }

    public String getDefaultTopicName()
    {
        return StubBrokerRule.TEST_TOPIC_NAME;
    }

    public int getDefaultPartitionId()
    {
        return StubBrokerRule.TEST_PARTITION_ID;
    }

    protected class ControllableZeebeClientImpl extends ZeebeClientImpl
    {

        public ControllableZeebeClientImpl(Properties properties)
        {
            super(properties);
        }
    }
}
