package com.x.farmer.bft.event.write;

import com.lmax.disruptor.EventFactory;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.message.WriteMessage;

public class WriteMsgListenerEventFactory implements EventFactory<CallBackListenerEvent<WriteMessage>> {

    public static final int RING_BUFFER = 256;

    @Override
    public CallBackListenerEvent<WriteMessage> newInstance() {
        return new CallBackListenerEvent<>();
    }
}
