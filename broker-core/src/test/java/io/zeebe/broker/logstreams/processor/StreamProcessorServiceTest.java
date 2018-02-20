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
package io.zeebe.broker.logstreams.processor;

public class StreamProcessorServiceTest
{
//
//    @Rule
//    public ExpectedException exception = ExpectedException.none();
//
//    @Rule
//    public MockStreamProcessorController<TestEvent> mockController = new MockStreamProcessorController<>(
//        TestEvent.class);
//
//    @Test
//    public void shouldRegisterVersionFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter eventFilter = controller.getEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { }, (m) -> m.protocolVersion(Integer.MAX_VALUE));
//
//        // then
//        exception.expect(RuntimeException.class);
//        exception.expectMessage("Cannot handle event with version newer than what is implemented by broker");
//
//        // when
//        eventFilter.applies(event);
//    }
//
//    @Test
//    public void shouldRegisterCustomRejectingFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//        streamProcessorService.eventFilter((m) -> false);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter eventFilter = controller.getEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { });
//
//        // when/then
//        assertThat(eventFilter.applies(event)).isFalse();
//    }
//
//    @Test
//    public void shouldRegisterCustomAcceptingFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//        streamProcessorService.eventFilter((m) -> true);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter eventFilter = controller.getEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { });
//
//        // when/then
//        assertThat(eventFilter.applies(event)).isTrue();
//    }
//
//    @Test
//    public void shouldRegisterReprocessingVersionFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter reprocessingEventFilter = controller.getReprocessingEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { }, (m) -> m.protocolVersion(Integer.MAX_VALUE));
//
//        // then
//        exception.expect(RuntimeException.class);
//        exception.expectMessage("Cannot handle event with version newer than what is implemented by broker");
//
//        // when
//        reprocessingEventFilter.applies(event);
//    }
//
//    @Test
//    public void shouldRegisterCustomReprocessingAcceptingFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//        streamProcessorService.reprocessingEventFilter(e -> true);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter reprocessingEventFilter = controller.getReprocessingEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { });
//
//        // when/then
//        assertThat(reprocessingEventFilter.applies(event)).isTrue();
//    }
//
//    @Test
//    public void shouldRegisterCustomReprocessingRejectingFilter()
//    {
//        // given
//        final StreamProcessorService streamProcessorService = new StreamProcessorService("foo", 1, mock(StreamProcessor.class));
//        injectMocks(streamProcessorService);
//        streamProcessorService.reprocessingEventFilter(e -> false);
//
//        streamProcessorService.start(mock(ServiceStartContext.class));
//
//        final StreamProcessorController controller = streamProcessorService.getStreamProcessorController();
//        final EventFilter reprocessingEventFilter = controller.getReprocessingEventFilter();
//
//        final LoggedEvent event = mockController.buildLoggedEvent(1L, (e) ->
//        { });
//
//        // when/then
//        assertThat(reprocessingEventFilter.applies(event)).isFalse();
//    }
//
//    protected void injectMocks(StreamProcessorService streamProcessorService)
//    {
//        final ActorScheduler actorScheduler = mock(ActorScheduler.class);
//        streamProcessorService.getActorSchedulerInjector().inject(actorScheduler);
//
//        final LogStream logStream = mock(LogStream.class);
//        when(logStream.getTopicName()).thenReturn(BufferUtil.wrapString(ClientApiRule.DEFAULT_TOPIC_NAME));
//        when(logStream.getPartitionId()).thenReturn(0);
//        streamProcessorService.getLogStreamInjector().inject(logStream);
//
//        streamProcessorService.getSnapshotStorageInjector().inject(mock(SnapshotStorage.class));
//    }
//
//    public static class TestEvent extends UnpackedObject
//    {
//    }
}
