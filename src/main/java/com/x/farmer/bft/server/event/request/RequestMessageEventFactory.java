package com.x.farmer.bft.server.event.request;

import com.lmax.disruptor.EventFactory;

public class RequestMessageEventFactory implements EventFactory<RequestMessageEvent> {

    public static final int RING_BUFFER = 1024 * 1024 * 16;

    @Override
    public RequestMessageEvent newInstance() {
        return new RequestMessageEvent();
    }
}
