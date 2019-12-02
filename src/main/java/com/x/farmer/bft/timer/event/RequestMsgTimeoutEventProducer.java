package com.x.farmer.bft.timer.event;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.message.RequestTimeoutMessage;

/**
 * 事件生产者
 *     用于生产事件
 */
public class RequestMsgTimeoutEventProducer {

    private final RingBuffer<RequestMsgTimeoutEvent> ringBuffer;

    public RequestMsgTimeoutEventProducer(RingBuffer<RequestMsgTimeoutEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<RequestMsgTimeoutEvent, RequestTimeoutMessage> TRANSLATOR =
            (requestMsgTimeoutEvent, sequence, requestMessage) -> requestMsgTimeoutEvent.setRequestTimeoutMessage(requestMessage);

    public void produce(RequestTimeoutMessage timeoutMessage) {
        ringBuffer.publishEvent(TRANSLATOR, timeoutMessage);
    }
}
