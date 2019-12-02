package com.x.farmer.bft.timer.event;

import com.x.farmer.bft.message.RequestTimeoutMessage;

public class RequestMsgTimeoutEvent {

    private RequestTimeoutMessage requestTimeoutMessage;

    public RequestTimeoutMessage getRequestTimeoutMessage() {
        return requestTimeoutMessage;
    }

    public void setRequestTimeoutMessage(RequestTimeoutMessage requestTimeoutMessage) {
        this.requestTimeoutMessage = requestTimeoutMessage;
    }
}
