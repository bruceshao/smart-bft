package com.x.farmer.bft.server.event.request;

import com.x.farmer.bft.message.RequestMessage;

public class RequestMessageEvent {

    private RequestMessage requestMessage;

    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    public void setRequestMessage(RequestMessage requestMessage) {
        this.requestMessage = requestMessage;
    }
}
