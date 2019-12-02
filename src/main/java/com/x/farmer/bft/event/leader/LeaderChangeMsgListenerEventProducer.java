package com.x.farmer.bft.event.leader;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.LeaderChangeMessage;
import com.x.farmer.bft.message.WriteMessage;

/**
 * 事件生产者
 *     用于生产事件
 */
public class LeaderChangeMsgListenerEventProducer {

    private final RingBuffer<CallBackListenerEvent<LeaderChangeMessage>> ringBuffer;

    public LeaderChangeMsgListenerEventProducer(RingBuffer<CallBackListenerEvent<LeaderChangeMessage>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<CallBackListenerEvent<LeaderChangeMessage>, CallBackListener<LeaderChangeMessage>> TRANSLATOR =
            (callBackListenerEvent, sequence, listener) -> callBackListenerEvent.setCallBackListener(listener);

    public void produce(CallBackListener<LeaderChangeMessage> listener) {
        ringBuffer.publishEvent(TRANSLATOR, listener);
    }
}
