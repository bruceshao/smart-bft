package com.x.farmer.bft.event.request;

import com.lmax.disruptor.EventFactory;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.message.WriteMessage;

public class TimeoutRequestMsgListenerEventFactory implements EventFactory<CallBackListenerEvent<RequestTimeoutMessage>> {

    public static final int RING_BUFFER = 256;

    @Override
    public CallBackListenerEvent<RequestTimeoutMessage> newInstance() {
        return new CallBackListenerEvent<>();
    }
}
