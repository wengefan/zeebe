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
package io.zeebe.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loggers
{

    public static final Logger CLUSTERING_LOGGER = LoggerFactory.getLogger("io.zeebe.broker.clustering");
    public static final Logger SERVICES_LOGGER = LoggerFactory.getLogger("io.zeebe.broker.services");
    public static final Logger SYSTEM_LOGGER = LoggerFactory.getLogger("io.zeebe.broker.system");
    public static final Logger TRANSPORT_LOGGER = LoggerFactory.getLogger("io.zeebe.broker.transport");

}
