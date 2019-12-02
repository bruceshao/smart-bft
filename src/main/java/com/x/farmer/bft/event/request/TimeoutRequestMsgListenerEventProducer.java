package com.x.farmer.bft.event.request;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.message.WriteMessage;

/**
 * 事件生产者
 *     用于生产事件
 */
public class TimeoutRequestMsgListenerEventProducer {

    private final RingBuffer<CallBackListenerEvent<RequestTimeoutMessage>> ringBuffer;

    public TimeoutRequestMsgListenerEventProducer(RingBuffer<CallBackListenerEvent<RequestTimeoutMessage>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<CallBackListenerEvent<RequestTimeoutMessage>, CallBackListener<RequestTimeoutMessage>> TRANSLATOR =
            (callBackListenerEvent, sequence, listener) -> callBackListenerEvent.setCallBackListener(listener);

    public void produce(CallBackListener<RequestTimeoutMessage> listener) {
        ringBuffer.publishEvent(TRANSLATOR, listener);
    }
}
