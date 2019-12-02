package com.x.farmer.bft.timer.event;

import com.lmax.disruptor.EventFactory;

public class RequestMsgTimeoutEventFactory implements EventFactory<RequestMsgTimeoutEvent> {

    public static final int RING_BUFFER = 1024;

    @Override
    public RequestMsgTimeoutEvent newInstance() {
        return new RequestMsgTimeoutEvent();
    }
}
