package com.x.farmer.bft.event.write;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.WriteMessage;

/**
 * 事件生产者
 *     用于生产事件
 */
public class WriteMsgListenerEventProducer {

    private final RingBuffer<CallBackListenerEvent<WriteMessage>> ringBuffer;

    public WriteMsgListenerEventProducer(RingBuffer<CallBackListenerEvent<WriteMessage>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<CallBackListenerEvent<WriteMessage>, CallBackListener<WriteMessage>> TRANSLATOR =
            (callBackListenerEvent, sequence, listener) -> callBackListenerEvent.setCallBackListener(listener);

    public void produce(CallBackListener<WriteMessage> listener) {
        ringBuffer.publishEvent(TRANSLATOR, listener);
    }
}
