package com.x.farmer.bft.server.event.propose;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.x.farmer.bft.message.ProposeMessage;
import com.x.farmer.bft.message.RequestMessage;

/**
 * 事件生产者
 *     用于生产事件
 */
public class ProposeMessageEventProducer {

    private final RingBuffer<ProposeMessageEvent> ringBuffer;

    public ProposeMessageEventProducer(RingBuffer<ProposeMessageEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<ProposeMessageEvent, ProposeMessage> TRANSLATOR =
            (proposeMessageEvent, sequence, proposeMessage) -> proposeMessageEvent.setProposeMessage(proposeMessage);

    public void produce(ProposeMessage proposeMessage) {
        ringBuffer.publishEvent(TRANSLATOR, proposeMessage);
    }
}
