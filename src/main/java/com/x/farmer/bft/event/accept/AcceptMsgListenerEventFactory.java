package com.x.farmer.bft.event.accept;

import com.lmax.disruptor.EventFactory;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.message.AcceptMessage;
import com.x.farmer.bft.message.WriteMessage;

public class AcceptMsgListenerEventFactory implements EventFactory<CallBackListenerEvent<AcceptMessage>> {

    public static final int RING_BUFFER = 256;

    @Override
    public CallBackListenerEvent<AcceptMessage> newInstance() {
        return new CallBackListenerEvent<>();
    }
}
