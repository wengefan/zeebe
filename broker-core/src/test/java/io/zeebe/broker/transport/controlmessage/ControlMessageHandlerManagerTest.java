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
package io.zeebe.broker.transport.controlmessage;

public class ControlMessageHandlerManagerTest
{
//    private static final ControlMessageType CONTROL_MESSAGE_TYPE = ControlMessageType.ADD_TASK_SUBSCRIPTION;
//    private static final byte[] CONTROL_MESSAGE_DATA = getBytes("foo");
//    private static final Duration TIMEOUT = Duration.ofSeconds(30);
//
//    private static final int REQ_STREAM_ID = 11;
//    private static final long REQ_REQUEST_ID = 13L;
//
//    private final UnsafeBuffer requestWriteBuffer = new UnsafeBuffer(new byte[1024]);
//
//    private final ControlMessageRequestHeaderDescriptor requestHeaderDescriptor = new ControlMessageRequestHeaderDescriptor();
//    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
//    private final ControlMessageRequestEncoder requestEncoder = new ControlMessageRequestEncoder();
//
//    @Mock
//    private Dispatcher mockControlMessageBuffer;
//
//    @Mock
//    private Subscription mockSubscription;
//
//    @Mock
//    private ActorScheduler mockTaskScheduler;
//    @Mock
//    private ActorReference mockActorRef;
//
//    @Mock
//    private ControlMessageHandler mockControlMessageHandler;
//
//    private ControlMessageHandlerManager manager;
//
//    @Mock
//    private BufferingServerOutput output;
//
//    @Before
//    public void init()
//    {
//        MockitoAnnotations.initMocks(this);
//
//        when(mockControlMessageBuffer.getSubscriptionByName("control-message-handler")).thenReturn(mockSubscription);
//
//        when(mockControlMessageHandler.getMessageType()).thenReturn(CONTROL_MESSAGE_TYPE);
//
//        output = new BufferingServerOutput();
//        manager = new ControlMessageHandlerManager(
//                output,
//                mockControlMessageBuffer,
//                TIMEOUT.toMillis(),
//                mockTaskScheduler,
//                Collections.singletonList(mockControlMessageHandler));
//
//        when(mockTaskScheduler.schedule(manager)).thenReturn(mockActorRef);
//
//        // fix the current time to calculate the timeout
//        ClockUtil.setCurrentTime(Instant.now());
//    }
//
//    @After
//    public void cleanUp()
//    {
//        ClockUtil.reset();
//    }
//
//    @Test
//    public void shouldGetRoleName()
//    {
//        assertThat(manager.name()).isEqualTo("control.message.handler");
//    }
//
//    @Test
//    public void shouldOpen()
//    {
//        // given
//        assertThat(manager.isOpen()).isFalse();
//
//        // when
//        final CompletableFuture<Void> future = manager.openAsync();
//        manager.doWork();
//
//        // then
//        assertThat(future).isCompleted();
//        assertThat(manager.isOpen()).isTrue();
//
//        verify(mockTaskScheduler).schedule(manager);
//    }
//
//    @Test
//    public void shouldNotOpenIfNotClosed()
//    {
//        opened();
//
//        assertThat(manager.isOpen()).isTrue();
//
//        // when try to open again
//        final CompletableFuture<Void> future = manager.openAsync();
//        manager.doWork();
//
//        // then
//        assertThat(future).isCompletedExceptionally();
//        assertThat(manager.isOpen()).isTrue();
//
//        verify(mockTaskScheduler, times(1)).schedule(manager);
//    }
//
//    @Test
//    public void shouldClose()
//    {
//        opened();
//
//        assertThat(manager.isClosed()).isFalse();
//
//        // when
//        final CompletableFuture<Void> future = manager.closeAsync();
//        manager.doWork();
//
//        // then
//        assertThat(future).isCompleted();
//        assertThat(manager.isClosed()).isTrue();
//
//        verify(mockActorRef).close();
//    }
//
//    @Test
//    public void shouldPollControlMessageBufferIfOpen()
//    {
//        // given
//        opened();
//
//        // when poll messages
//        manager.doWork();
//        manager.doWork();
//
//        // then
//        assertThat(manager.isOpen()).isTrue();
//
//        verify(mockSubscription, times(2)).poll(any(FragmentHandler.class), eq(1));
//    }
//
//    @Test
//    public void shouldHandlePolledControlMessage()
//    {
//        // given
//        opened();
//
//        when(mockSubscription.poll(any(FragmentHandler.class), eq(1))).thenAnswer(pollControlMessage(CONTROL_MESSAGE_TYPE));
//
//        // when poll a message
//        manager.doWork();
//
//        // then
//        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
//        final ArgumentCaptor<BrokerEventMetadata> metadataCaptor = ArgumentCaptor.forClass(BrokerEventMetadata.class);
//
//        verify(mockControlMessageHandler).handle(anyInt(), bufferCaptor.capture(), metadataCaptor.capture());
//
//        assertThatBuffer(bufferCaptor.getValue()).hasBytes(CONTROL_MESSAGE_DATA);
//
//        final BrokerEventMetadata metadata = metadataCaptor.getValue();
//        assertThat(metadata.getRequestStreamId()).isEqualTo(REQ_STREAM_ID);
//        assertThat(metadata.getRequestId()).isEqualTo(REQ_REQUEST_ID);
//    }
//
//    @Test
//    public void shouldWaitUntilPolledControlMessageIsHandled()
//    {
//        // given a polled message
//        opened();
//
//        when(mockSubscription.poll(any(FragmentHandler.class), eq(1))).thenAnswer(pollControlMessage(CONTROL_MESSAGE_TYPE));
//
//        final CompletableFuture<Void> spyFuture = spy(new CompletableFuture<Void>());
//        when(mockControlMessageHandler.handle(anyInt(), any(DirectBuffer.class), any(BrokerEventMetadata.class))).thenReturn(spyFuture);
//
//        manager.doWork();
//
//        // when wait for completion
//        manager.doWork();
//        manager.doWork();
//        manager.doWork();
//
//        verify(mockSubscription, times(1)).poll(any(FragmentHandler.class), eq(1));
//
//        spyFuture.complete(null);
//
//        // and continue polling
//        manager.doWork();
//        manager.doWork();
//
//        // then
//        assertThat(manager.isOpen()).isTrue();
//
//        verify(mockSubscription, times(2)).poll(any(FragmentHandler.class), eq(1));
//    }
//
//    @Test
//    public void shouldWriteErrorResponseIfHandleControlMessageTakesLongerThanTimeout()
//    {
//        // given a polled message
//        opened();
//
//        when(mockSubscription.poll(any(FragmentHandler.class), eq(1))).thenAnswer(pollControlMessage(CONTROL_MESSAGE_TYPE));
//
//        final CompletableFuture<Void> spyFuture = spy(new CompletableFuture<Void>());
//        when(mockControlMessageHandler.handle(anyInt(), any(DirectBuffer.class), any(BrokerEventMetadata.class))).thenReturn(spyFuture);
//
//        manager.doWork();
//
//        // when wait for completion until timeout
//        manager.doWork();
//
//        ClockUtil.setCurrentTime(ClockUtil.getCurrentTime().plus(TIMEOUT));
//
//        // and continue polling
//        manager.doWork();
//        manager.doWork();
//
//        // then
//        assertThat(manager.isOpen()).isTrue();
//
//        assertThat(output.getSentResponses()).hasSize(1);
//
//        final ErrorResponseDecoder errorResponse = output.getAsErrorResponse(0);
//
//        assertThat(errorResponse.errorCode()).isEqualTo(ErrorCode.REQUEST_TIMEOUT);
//        assertThat(errorResponse.errorData()).isEqualTo("Timeout while handle control message.");
//
//        verify(spyFuture, times(2)).isDone();
//        verify(mockSubscription, times(2)).poll(any(FragmentHandler.class), eq(1));
//    }
//
//    @Test
//    public void shouldWriteErrorResponseIfNotSupportedMessageType()
//    {
//        // given
//        opened();
//
//        when(mockSubscription.poll(any(FragmentHandler.class), eq(1))).thenAnswer(pollControlMessage(ControlMessageType.SBE_UNKNOWN));
//
//        // when handle the message
//        manager.doWork();
//        manager.doWork();
//        // and continue polling
//        manager.doWork();
//
//        // then
//        assertThat(manager.isOpen()).isTrue();
//
//        assertThat(output.getSentResponses()).hasSize(1);
//
//        final ErrorResponseDecoder errorResponse = output.getAsErrorResponse(0);
//
//        assertThat(errorResponse.errorCode()).isEqualTo(ErrorCode.MESSAGE_NOT_SUPPORTED);
//        assertThat(errorResponse.errorData()).isEqualTo("Cannot handle control message with type 'NULL_VAL'.");
//        verify(mockSubscription, times(2)).poll(any(FragmentHandler.class), eq(1));
//    }
//
//    private void opened()
//    {
//        manager.openAsync();
//        manager.doWork();
//    }
//
//    private Answer<?> pollControlMessage(ControlMessageType type)
//    {
//        return invocation ->
//        {
//            final FragmentHandler fragmentHandler = (FragmentHandler) invocation.getArguments()[0];
//
//            int offset = 0;
//
//            requestHeaderDescriptor
//                .wrap(requestWriteBuffer, offset)
//                .streamId(REQ_STREAM_ID)
//                .requestId(REQ_REQUEST_ID);
//
//            offset += ControlMessageRequestHeaderDescriptor.headerLength();
//
//            messageHeaderEncoder
//                .wrap(requestWriteBuffer, offset)
//                .blockLength(requestEncoder.sbeBlockLength())
//                .templateId(requestEncoder.sbeTemplateId())
//                .schemaId(requestEncoder.sbeSchemaId())
//                .version(requestEncoder.sbeSchemaVersion());
//
//            offset += messageHeaderEncoder.encodedLength();
//
//            requestEncoder
//                .wrap(requestWriteBuffer, offset)
//                .messageType(type)
//                .putData(CONTROL_MESSAGE_DATA, 0, CONTROL_MESSAGE_DATA.length);
//
//            fragmentHandler.onFragment(requestWriteBuffer, 0, offset, 0, false);
//
//            return 1;
//        };
//    }
}
