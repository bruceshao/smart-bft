package com.x.farmer.bft.server.event.propose;

import com.x.farmer.bft.message.ProposeMessage;

public class ProposeMessageEvent {

    private ProposeMessage proposeMessage;

    public ProposeMessage getProposeMessage() {
        return proposeMessage;
    }

    public void setProposeMessage(ProposeMessage proposeMessage) {
        this.proposeMessage = proposeMessage;
    }
}
