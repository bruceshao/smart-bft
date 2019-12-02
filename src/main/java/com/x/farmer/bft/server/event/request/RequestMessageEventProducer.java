package com.x.farmer.bft.server.event.request;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.message.RequestMessage;


/**
 * 事件生产者
 *     用于生产事件
 */
public class RequestMessageEventProducer {

    private final RingBuffer<RequestMessageEvent> ringBuffer;

    public RequestMessageEventProducer(RingBuffer<RequestMessageEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<RequestMessageEvent, RequestMessage> TRANSLATOR =
            (requestMessageEvent, sequence, requestMessage) -> requestMessageEvent.setRequestMessage(requestMessage);

    public void produce(RequestMessage requestMessage) {
        ringBuffer.publishEvent(TRANSLATOR, requestMessage);
    }
}
