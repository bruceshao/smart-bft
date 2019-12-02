package com.x.farmer.bft.message;

import com.x.farmer.bft.util.ByteUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WriteMessage extends AbstractMessage {

    private byte[] toBytes;

    private byte[] agreement;

    private Map<Integer, List<Long>> idAndSequences = new ConcurrentHashMap<>();

    public WriteMessage(int id, long sequence, long key, byte[] agreement) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.agreement = agreement;
    }

    public WriteMessage(int id, long sequence, long key, byte[] agreement, Map<Integer, List<Long>> idAndSequences) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.agreement = agreement;
        this.idAndSequences = idAndSequences;
    }

    public void add(int id, long sequence) {
        idAndSequences.computeIfAbsent(id, k -> new ArrayList<>()).add(sequence);
    }

    public byte[] getAgreement() {
        return agreement;
    }

    public Map<Integer, List<Long>> getIdAndSequences() {
        return idAndSequences;
    }

    @Override
    public MessageType type() {
        return MessageType.WRITE;
    }

    @Override
    public byte[] toBytes() {

        lock.lock();

        try {
            if (toBytes == null) {
                // 格式： |argeement-length|agreement|idAndSequences|
                toBytes = ByteUtils.mergeAndAddLength(agreement, idAndSequencesBytes());
            }

            return toBytes;

        } finally {
            lock.unlock();
        }
    }

    private byte[] idAndSequencesBytes() {

        List<byte[]> totalByteList = new ArrayList<>();

        int totalBytesLength = 0;

        for (Map.Entry<Integer, List<Long>> entry : idAndSequences.entrySet()) {

            int id = entry.getKey();

            List<Long> sequences = entry.getValue();

            if (!CollectionUtils.isEmpty(sequences)) {

                int entryLength = Integer.BYTES + sequences.size() * Long.BYTES;

                byte[] entryBytes = new byte[entryLength + Integer.BYTES];

                // 先写入长度，在写入ID，再遍历写入Sequence
                System.arraycopy(ByteUtils.intToBytes(entryLength), 0, entryBytes, 0, Integer.BYTES);

                System.arraycopy(ByteUtils.intToBytes(id), 0, entryBytes, Integer.BYTES, Integer.BYTES);

                int position = Integer.BYTES * 2;

                for (Long seq : sequences) {
                    System.arraycopy(ByteUtils.longToBytes(seq), 0, entryBytes, position, Long.BYTES);
                    position += Long.BYTES;
                }

                totalByteList.add(entryBytes);
                totalBytesLength += entryBytes.length;
            }
        }

        if (!totalByteList.isEmpty()) {

            byte[] totalBytes = new byte[totalBytesLength];

            int position = 0;

            for (byte[] entryBytes : totalByteList) {

                System.arraycopy(entryBytes, 0, totalBytes, position, entryBytes.length);

                position += entryBytes.length;
            }

            return totalBytes;
        }

        return null;
    }

    /**
     * 转换为WriteMessage对象
     *
     * @param id
     * @param sequence
     * @param key
     * @param totalBytes
     *     agreement+idAndSequences对应的字节数组，格式：|agreement-length|agreement|idAndSequences|
     * @return
     */
    public static WriteMessage toMessage(int id, long sequence, long key, byte[] totalBytes) {

        byte[] lengthBytes = new byte[Integer.BYTES];

        int position = 0;

        System.arraycopy(totalBytes, position, lengthBytes, 0, lengthBytes.length);

        position += lengthBytes.length;

        int agreeLength = ByteUtils.bytesToInt(lengthBytes);

        byte[] agreement = new byte[agreeLength];

        System.arraycopy(totalBytes, position, agreement, 0, agreeLength);

        position += agreeLength;

        byte[] idAndSequencesBytes = new byte[totalBytes.length - lengthBytes.length - agreeLength];

        System.arraycopy(totalBytes, position, idAndSequencesBytes, 0, idAndSequencesBytes.length);

        // 将idAndSequencesBytes转换为Map<Integer, List<Long>>对象
        // 格式：|entry-length|id|sequence...| -> |entry-length|id|sequence...|

        Map<Integer, List<Long>> idAndSequences = new ConcurrentHashMap<>();

        position = 0;

        while (position < idAndSequencesBytes.length) {

            byte[] entryLengthBytes = new byte[Integer.BYTES];

            System.arraycopy(idAndSequencesBytes, position, entryLengthBytes, 0, entryLengthBytes.length);

            position += entryLengthBytes.length;

            int entryLength = ByteUtils.bytesToInt(entryLengthBytes);

            byte[] entryBytes = new byte[entryLength];

            System.arraycopy(idAndSequencesBytes, position, entryBytes, 0, entryLength);

            position += entryLength;

            int entryPosition = 0;

            byte[] idBytes = new byte[Integer.BYTES];

            System.arraycopy(entryBytes, entryPosition, idBytes, 0, idBytes.length);

            entryPosition += idBytes.length;

            int entryId = ByteUtils.bytesToInt(idBytes);

            List<Long> sequences = new ArrayList<>();

            while (entryPosition < entryBytes.length) {

                byte[] seqBytes = new byte[Long.BYTES];

                System.arraycopy(entryBytes, entryPosition, seqBytes, 0, seqBytes.length);

                entryPosition += seqBytes.length;

                sequences.add(ByteUtils.bytesToLong(seqBytes));
            }

            idAndSequences.put(entryId, sequences);
        }


        return new WriteMessage(id, sequence, key, agreement, idAndSequences);
    }
}
