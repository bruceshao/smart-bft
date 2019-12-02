package com.x.farmer.bft.message;

import com.x.farmer.bft.util.ByteUtils;

public class LeaderChangeMessage extends AbstractMessage {

    private byte[] toBytes;

    private int newLeader;

    public LeaderChangeMessage(int id, long sequence, long key, int newLeader) {
        this.id = id;
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

    @Override
    public MessageType type() {
        return MessageType.LEADER_CHANGE;
    }

    @Override
    public byte[] toBytes() {

        lock.lock();

        try {
            if (toBytes == null) {
                // 格式： newLeaderID
                toBytes = ByteUtils.intToBytes(newLeader);
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

        return new LeaderChangeMessage(id, sequence, key, ByteUtils.bytesToInt(totalBytes));
    }
}
