package com.x.farmer.bft.event.leader;

import com.lmax.disruptor.EventFactory;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.message.LeaderChangeMessage;
import com.x.farmer.bft.message.WriteMessage;

public class LeaderChangeMsgListenerEventFactory implements EventFactory<CallBackListenerEvent<LeaderChangeMessage>> {

    public static final int RING_BUFFER = 256;

    @Override
    public CallBackListenerEvent<LeaderChangeMessage> newInstance() {
        return new CallBackListenerEvent<>();
    }
}
