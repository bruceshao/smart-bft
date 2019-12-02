package com.x.farmer.bft.message;

public class ResponseMessage extends AbstractMessage {

    private byte[] body;

    public ResponseMessage(int id, long sequence) {
        this.id = id;
        this.sequence = sequence;
    }

    public ResponseMessage(int id, long sequence, byte[] body) {
        this.id = id;
        this.sequence = sequence;
        this.body = body;
    }

    public ResponseMessage(int id, long sequence, long key) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
    }

    public ResponseMessage(int id, long sequence, long key, byte[] body) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public MessageType type() {
        return MessageType.RESPONSE;
    }

    @Override
    public byte[] toBytes() {
        return body;
    }
}
