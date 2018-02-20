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
package io.zeebe.broker.clustering.management.memberList;

import static io.zeebe.broker.clustering.management.memberList.GossipEventCreationHelper.*;
import static io.zeebe.raft.state.RaftState.LEADER;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.handler.Topology;
import io.zeebe.broker.clustering.management.ClusterManagerContext;
import io.zeebe.broker.clustering.management.OnOpenLogStreamListener;
import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.gossip.Gossip;
import io.zeebe.gossip.GossipCustomEventListener;
import io.zeebe.gossip.GossipMembershipListener;
import io.zeebe.gossip.membership.Member;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.DeferredCommandContext;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;

public class ClusterMemberListManager implements RaftStateListener, OnOpenLogStreamListener
{
    public static final Logger LOG = Loggers.CLUSTERING_LOGGER;
    public static final DirectBuffer API_EVENT_TYPE = BufferUtil.wrapString("apis");
    public static final DirectBuffer MEMBER_RAFT_STATES_EVENT_TYPE = BufferUtil.wrapString("memberRaftStates");

    private final ClusterManagerContext context;
    private TransportComponentCfg transportComponentCfg;
    private final List<MemberRaftComposite> deadMembers;
    private final Consumer<SocketAddress> updatedMemberConsumer;
    private final TopologyCreator topologyCreator;

    // buffers
    private final ExpandableArrayBuffer apiAddressBuffer;
    private final ExpandableArrayBuffer memberRaftStatesBuffer;

    private final ActorControl actor;

    public ClusterMemberListManager(ClusterManagerContext context,
                                    ActorControl actorControl,
                                    TransportComponentCfg transportComponentCfg,
                                    Consumer<SocketAddress> updatedMemberConsumer)
    {
        this.context = context;
        this.deadMembers = new ArrayList<>();
        this.actor = actorControl;
        this.transportComponentCfg = transportComponentCfg;
        this.updatedMemberConsumer = updatedMemberConsumer;

        final MemberListService memberListService = context.getMemberListService();
        final String defaultHost = transportComponentCfg.host;
        memberListService.add(new Member(transportComponentCfg.managementApi.toSocketAddress(defaultHost)));
        memberListService.setApis(transportComponentCfg.clientApi.toSocketAddress(defaultHost), transportComponentCfg.replicationApi.toSocketAddress(defaultHost),
                                  transportComponentCfg.managementApi.toSocketAddress(defaultHost));

        context.getGossip()
               .addMembershipListener(new MembershipListener());
        context.getGossip()
               .addCustomEventListener(API_EVENT_TYPE, new APIEventListener());
        context.getGossip()
               .addCustomEventListener(MEMBER_RAFT_STATES_EVENT_TYPE, new MemberRaftStatesEventListener());

        // sync handlers
        context.getGossip()
               .registerSyncRequestHandler(API_EVENT_TYPE, new APISyncHandler(actorControl, context));
        context.getGossip()
               .registerSyncRequestHandler(MEMBER_RAFT_STATES_EVENT_TYPE, new MemberRaftStatesSyncHandler(actorControl, context));

        topologyCreator = new TopologyCreator(context);

        this.apiAddressBuffer = new ExpandableArrayBuffer();
        this.memberRaftStatesBuffer = new ExpandableArrayBuffer();
    }

    public void publishNodeAPIAddresses()
    {
        final Gossip gossip = context.getGossip();
        final String defaultHost = transportComponentCfg.host;
        final DirectBuffer payload = writeAPIAddressesIntoBuffer(transportComponentCfg.managementApi.toSocketAddress(defaultHost),
                                                                 transportComponentCfg.replicationApi.toSocketAddress(defaultHost),
                                                                 transportComponentCfg.clientApi.toSocketAddress(defaultHost),
                                                                 apiAddressBuffer);
        gossip.publishEvent(API_EVENT_TYPE, payload);
    }

    public ActorFuture<Topology> createTopology()
    {
        return actor.call(topologyCreator::createTopology);

    }

    private class MembershipListener implements GossipMembershipListener
    {
        @Override
        public void onAdd(Member member)
        {
            final MemberRaftComposite newMember = new MemberRaftComposite(member);
            actor.call(() ->
            {
                LOG.debug("Add member {} to member list.", newMember);
                MemberRaftComposite memberRaftComposite = newMember;
                final int indexOfDeadMember = deadMembers.indexOf(newMember);

                if (indexOfDeadMember > -1)
                {
                    memberRaftComposite = deadMembers.remove(indexOfDeadMember);
                    LOG.debug("Re-add dead member {} to member list", memberRaftComposite);
                }
                context.getMemberListService()
                       .add(memberRaftComposite);
            });
        }

        @Override
        public void onRemove(Member member)
        {
            final SocketAddress memberAddress = member.getAddress();
            actor.call(() ->
            {
                final MemberRaftComposite removedMember = context.getMemberListService()
                                                                 .remove(memberAddress);
                LOG.debug("Remove member {} from member list.", removedMember);
                deadMembers.add(removedMember);

                deactivateRemote(context.getManagementClient(), removedMember.getManagementApi());
                deactivateRemote(context.getReplicationClient(), removedMember.getReplicationApi());
            });
        }
    }

    protected void deactivateRemote(ClientTransport transport, SocketAddress address)
    {
        final RemoteAddress managementRemote = transport.getRemoteAddress(address);
        if (managementRemote != null)
        {
            transport.deactivateRemoteAddress(managementRemote);
        }
    }

    private final class APIEventListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress socketAddress, DirectBuffer directBuffer)
        {
            final DirectBuffer savedBuffer = BufferUtil.cloneBuffer(directBuffer);
            final SocketAddress savedSocketAddress = new SocketAddress(socketAddress);
            actor.call(() ->
            {
                LOG.debug("Received API event from member {}.", savedSocketAddress);

                final SocketAddress managementApi = new SocketAddress();
                final SocketAddress clientApi = new SocketAddress();
                final SocketAddress replicationApi = new SocketAddress();

                int offset = 0;
                // management
                offset = readFromBufferIntoSocketAddress(offset, savedBuffer, managementApi);
                // client
                offset = readFromBufferIntoSocketAddress(offset, savedBuffer, clientApi);
                // replication
                readFromBufferIntoSocketAddress(offset, savedBuffer, replicationApi);

                final boolean success = context.getMemberListService()
                                               .setApis(clientApi, replicationApi, managementApi);

                LOG.debug("Setting API's for member {} was {}successful.", savedSocketAddress, success ? "" : "not ");

                updatedMemberConsumer.accept(savedSocketAddress);

                context.getManagementClient().registerRemoteAddress(managementApi);
                context.getReplicationClient().registerRemoteAddress(replicationApi);
            });
        }
    }

    private final class MemberRaftStatesEventListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress socketAddress, DirectBuffer directBuffer)
        {
            final DirectBuffer savedBuffer = BufferUtil.cloneBuffer(directBuffer);
            final SocketAddress savedSocketAddress = new SocketAddress(socketAddress);
            actor.call(() ->
            {
                LOG.debug("Received raft state change event for member {}", savedSocketAddress);
                final MemberRaftComposite member = context.getMemberListService()
                                                          .getMember(savedSocketAddress);

                if (member == null)
                {
                    LOG.debug("Member {} does not exist. Maybe dead? List of dead members: {}", savedSocketAddress, deadMembers);
                }
                else
                {
                    updateMemberWithNewRaftState(member, savedBuffer);

                    LOG.debug("Handled raft state change event for member {} - local member state: {}", savedSocketAddress, context.getMemberListService());
                }
            });
        }
    }

    @Override
    public void onOpenLogStreamService(LogStream logStream)
    {
        final int partitionId = logStream.getPartitionId();
        final DirectBuffer savedTopicName = BufferUtil.cloneBuffer(logStream.getTopicName());

        actor.call(() ->  updateTopologyOnRaftStateChangeForPartition(LEADER, partitionId, savedTopicName));
    }

    @Override
    public void onStateChange(int partitionId, DirectBuffer topicName, SocketAddress socketAddress, RaftState raftState)
    {
        final DirectBuffer savedTopicName = BufferUtil.cloneBuffer(topicName);
        actor.call(() ->
        {
            if (raftState == RaftState.FOLLOWER)
            {
                updateTopologyOnRaftStateChangeForPartition(raftState, partitionId, savedTopicName);
            }
        });
    }

    private void updateTopologyOnRaftStateChangeForPartition(RaftState raftState, int partitionId, DirectBuffer savedTopicName)
    {
        final MemberRaftComposite member = context.getMemberListService()
                                                  .getMember(transportComponentCfg.managementApi.toSocketAddress(transportComponentCfg.host));

        // update raft state in member list
        member.updateRaft(partitionId, savedTopicName, raftState);
        LOG.trace("On raft state change for {} - local member states: {}", member.getMember().getAddress(), context.getMemberListService());

        // send complete list of partition where I'm a follower or leader
        final List<RaftStateComposite> rafts = member.getRafts();
        final DirectBuffer payload = writeRaftsIntoBuffer(rafts, memberRaftStatesBuffer);

        LOG.trace("Publish event for partition {} state change {}", partitionId, raftState);

        context.getGossip()
               .publishEvent(MEMBER_RAFT_STATES_EVENT_TYPE, payload);
    }
}
