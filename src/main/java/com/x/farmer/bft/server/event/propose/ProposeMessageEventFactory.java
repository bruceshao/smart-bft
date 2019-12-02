package com.x.farmer.bft.server.event.propose;

import com.lmax.disruptor.EventFactory;

public class ProposeMessageEventFactory implements EventFactory<ProposeMessageEvent> {

    public static final int RING_BUFFER = 1024;

    @Override
    public ProposeMessageEvent newInstance() {
        return new ProposeMessageEvent();
    }
}
