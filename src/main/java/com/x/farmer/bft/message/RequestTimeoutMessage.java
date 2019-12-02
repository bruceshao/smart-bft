package com.x.farmer.bft.message;

public class RequestTimeoutMessage extends RequestMessage {

    public RequestTimeoutMessage(int id, long sequence, long key, byte[] body) {
        super(id, sequence, key, body);
    }

    @Override
    public MessageType type() {
        return MessageType.REQUEST_TIMEOUT;
    }
}
