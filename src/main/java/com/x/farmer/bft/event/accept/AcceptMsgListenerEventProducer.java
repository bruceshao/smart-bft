package com.x.farmer.bft.event.accept;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.AcceptMessage;


/**
 * 事件生产者
 *     用于生产事件
 */
public class AcceptMsgListenerEventProducer {

    private final RingBuffer<CallBackListenerEvent<AcceptMessage>> ringBuffer;

    public AcceptMsgListenerEventProducer(RingBuffer<CallBackListenerEvent<AcceptMessage>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<CallBackListenerEvent<AcceptMessage>, CallBackListener<AcceptMessage>> TRANSLATOR =
            (callBackListenerEvent, sequence, listener) -> callBackListenerEvent.setCallBackListener(listener);

    public void produce(CallBackListener<AcceptMessage> listener) {
        ringBuffer.publishEvent(TRANSLATOR, listener);
    }
}
