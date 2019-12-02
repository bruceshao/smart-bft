package com.x.farmer.bft.message;


import java.nio.charset.StandardCharsets;

public class HeartBeatMessage extends AbstractMessage {

    private static final long HEARTBEAT_KEY = 1024L;

    /**
     * 统一的心跳信息字符串
     */
    private static final String HEARTBEAT_DATA = "IAmHeartBeat";

    private static final byte[] HEARTBEAT_BYTES = HEARTBEAT_DATA.getBytes(StandardCharsets.UTF_8);

    public HeartBeatMessage(int id) {
        this.id = id;
        this.sequence = 0L;
        this.key = HEARTBEAT_KEY;
    }

    public HeartBeatMessage(int id, long sequence) {
        this.id = id;
        this.sequence = sequence;
        this.key = HEARTBEAT_KEY;
    }

    @Override
    public MessageType type() {
        return MessageType.HEARTBEAT;
    }

    @Override
    public byte[] toBytes() {
        return HEARTBEAT_BYTES;
    }

    public static HeartBeatMessage toMessage(int id, long sequence, long key, byte[] body) {

        return new HeartBeatMessage(id, sequence);
    }
}
