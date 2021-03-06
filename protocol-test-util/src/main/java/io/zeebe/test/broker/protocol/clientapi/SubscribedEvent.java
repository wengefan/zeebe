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
package io.zeebe.test.broker.protocol.clientapi;

import static io.zeebe.protocol.clientapi.SubscribedEventDecoder.eventHeaderLength;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import io.zeebe.protocol.clientapi.*;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.util.buffer.BufferReader;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.io.DirectBufferInputStream;

public class SubscribedEvent implements BufferReader
{
    protected final RawMessage rawMessage;

    protected MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    protected SubscribedEventDecoder bodyDecoder = new SubscribedEventDecoder();

    protected Map<String, Object> event;

    protected MsgPackHelper msgPackHelper = new MsgPackHelper();

    public SubscribedEvent(RawMessage rawMessage)
    {
        this.rawMessage = rawMessage;
        final DirectBuffer buffer = rawMessage.getMessage();
        wrap(buffer, 0, buffer.capacity());
    }

    public int partitionId()
    {
        return bodyDecoder.partitionId();
    }

    public long position()
    {
        return bodyDecoder.position();
    }

    public long key()
    {
        return bodyDecoder.key();
    }

    public long subscriberKey()
    {
        return bodyDecoder.subscriberKey();
    }

    public SubscriptionType subscriptionType()
    {
        return bodyDecoder.subscriptionType();
    }

    public EventType eventType()
    {
        return bodyDecoder.eventType();
    }

    public Map<String, Object> event()
    {
        return event;
    }

    public RawMessage getRawMessage()
    {
        return rawMessage;
    }

    @Override
    public void wrap(DirectBuffer responseBuffer, int offset, int length)
    {
        headerDecoder.wrap(responseBuffer, offset);

        if (headerDecoder.templateId() != bodyDecoder.sbeTemplateId())
        {
            throw new RuntimeException("Unexpected response from broker.");
        }

        bodyDecoder.wrap(responseBuffer, offset + headerDecoder.encodedLength(), headerDecoder.blockLength(), headerDecoder.version());

        final int eventLength = bodyDecoder.eventLength();
        final int eventOffset = bodyDecoder.limit() + eventHeaderLength();

        try (InputStream is = new DirectBufferInputStream(responseBuffer, offset + eventOffset, eventLength))
        {
            event = msgPackHelper.readMsgPack(is);
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }

    }

}
