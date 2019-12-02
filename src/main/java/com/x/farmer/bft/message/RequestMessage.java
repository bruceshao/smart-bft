package com.x.farmer.bft.message;


import com.x.farmer.bft.util.ByteUtils;

public class RequestMessage extends AbstractMessage {

    public static final int STATE_DEFAULT = 0;

    public static final int STATE_PROPOSED = 1;

    public static final int STATE_ACCEPTED = 3;

    private int state = STATE_DEFAULT;

    private byte[] body;

    public RequestMessage(int id, long sequence) {
        this.id = id;
        this.sequence = sequence;
    }

    public RequestMessage(int id, long sequence, byte[] body) {
        this.id = id;
        this.sequence = sequence;
        this.body = body;
    }

    public RequestMessage(int id, long sequence, long key) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
    }

    public RequestMessage(int id, long sequence, long key, byte[] body) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.body = body;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public RequestMessage updateState(int state) {
        setState(state);
        return this;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] toTotalBytes() {
        // 格式：|length|id|sequence|body|
        byte[] idBytes = ByteUtils.intToBytes(id);
        byte[] seqBytes = ByteUtils.longToBytes(sequence);
        byte[] totalBytes = new byte[Integer.BYTES + idBytes.length + seqBytes.length + body.length];
        // 写入总长度
        System.arraycopy(ByteUtils.intToBytes(totalBytes.length - Integer.BYTES), 0, totalBytes, 0, Integer.BYTES);
        // 写入ID
        System.arraycopy(idBytes, 0, totalBytes, Integer.BYTES, idBytes.length);
        // 写入Sequence
        System.arraycopy(seqBytes, 0, totalBytes, idBytes.length + Integer.BYTES, seqBytes.length);
        // 写入Body
        System.arraycopy(body, 0, totalBytes, idBytes.length + seqBytes.length + Integer.BYTES, body.length);
        return totalBytes;
    }

    @Override
    public MessageType type() {
        return MessageType.REQUEST;
    }

    @Override
    public byte[] toBytes() {

        // 格式：|body|
        return body;
    }

    public static RequestMessage toMessage(int id, long sequence, long key, byte[] body) {

        return new RequestMessage(id, sequence, key, body);
    }

    /**
     * 字节数组转换为RequestMessage对象
     *
     * @param totalBytes
     *     字节数组，格式：|id|sequence|body|
     *
     * @return
     */
    public static RequestMessage toMessage(byte[] totalBytes) {

        int position = 0;

        byte[] idBytes = new byte[Integer.BYTES];

        System.arraycopy(totalBytes, position, idBytes, 0, idBytes.length);

        position += idBytes.length;

        byte[] seqBytes = new byte[Long.BYTES];

        System.arraycopy(totalBytes, position, seqBytes, 0, seqBytes.length);

        position += seqBytes.length;

        byte[] body = new byte[totalBytes.length - idBytes.length - seqBytes.length];

        System.arraycopy(totalBytes, position, body, 0, body.length);

        return new RequestMessage(ByteUtils.bytesToInt(idBytes), ByteUtils.bytesToLong(seqBytes), body);
    }
}
