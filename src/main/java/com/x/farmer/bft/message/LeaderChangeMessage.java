package com.x.farmer.bft.message;

import com.x.farmer.bft.util.ByteUtils;

public class LeaderChangeMessage extends AbstractMessage {

    private static final int BODY_LENGTH = Integer.BYTES * 2;

    private byte[] toBytes;

    private int msgId = -1;

    private int newLeader;

    public LeaderChangeMessage(int id, long sequence, long key, int newLeader) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.newLeader = newLeader;
    }

    public LeaderChangeMessage(int id, int msgId, long sequence, long key, int newLeader) {
        this.id = id;
        this.msgId = msgId;
        this.sequence = sequence;
        this.key = key;
        this.newLeader = newLeader;
    }

    public int getNewLeader() {
        return newLeader;
    }

    public void setNewLeader(int newLeader) {
        this.newLeader = newLeader;
    }

    public int getMsgId() {
        return msgId;
    }

    @Override
    public MessageType type() {
        return MessageType.LEADER_CHANGE;
    }

    @Override
    public byte[] toBytes() {

        lock.lock();

        try {
            if (toBytes == null) {
                // 格式： |msgId|newLeaderID|
                toBytes = new byte[BODY_LENGTH];
                System.arraycopy(ByteUtils.intToBytes(msgId), 0, toBytes, 0, Integer.BYTES);
                System.arraycopy(ByteUtils.intToBytes(newLeader), 0, toBytes, Integer.BYTES, Integer.BYTES);
            }

            return toBytes;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 转换为LeaderChange对象
     *
     * @param id
     * @param sequence
     * @param key
     * @param totalBytes
     *     newLeader
     * @return
     */
    public static LeaderChangeMessage toMessage(int id, long sequence, long key, byte[] totalBytes) {

        if (totalBytes == null || totalBytes.length != BODY_LENGTH) {
            throw new IllegalStateException("Serialize Message is Valid !!!");
        }
        byte[] msgIdBytes = new byte[Integer.BYTES], newLeaderBytes = new byte[Integer.BYTES];
        System.arraycopy(totalBytes, 0, msgIdBytes, 0, Integer.BYTES);
        System.arraycopy(totalBytes, Integer.BYTES, newLeaderBytes, 0, Integer.BYTES);

        return new LeaderChangeMessage(id, ByteUtils.bytesToInt(msgIdBytes), sequence, key, ByteUtils.bytesToInt(newLeaderBytes));
    }
}
