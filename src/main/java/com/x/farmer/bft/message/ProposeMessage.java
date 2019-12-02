package com.x.farmer.bft.message;

import com.x.farmer.bft.util.ByteUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProposeMessage extends AbstractMessage {

    private byte[] toBytes;

    private List<RequestMessage> requestMessages = new ArrayList<>();

    public ProposeMessage(int id, long sequence, long key) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
    }

    public ProposeMessage(int id, long sequence, long key, List<RequestMessage> requestMessages) {
        this.id = id;
        this.sequence = sequence;
        this.key = key;
        this.requestMessages = requestMessages;
    }

    public void setRequestMessages(List<RequestMessage> requestMessages) {
        this.requestMessages = requestMessages;
    }

    public List<RequestMessage> getRequestMessages() {
        return requestMessages;
    }

    public void addMessage(RequestMessage requestMessage) {
        requestMessages.add(requestMessage);
    }

    @Override
    public MessageType type() {
        return MessageType.PROPOSE;
    }

    @Override
    public byte[] toBytes() {

        lock.lock();

        try {
            if (toBytes == null) {
                toBytes = encode();
            }

            return toBytes;

        } finally {
            lock.unlock();
        }
    }

    private byte[] encode() {

        List<byte[]> requestBytes = new ArrayList<>(requestMessages.size());

        int totalLength = 0;

        for (RequestMessage rm : requestMessages) {
            byte[] rmBytes = rm.toTotalBytes();
            totalLength += rmBytes.length;
            requestBytes.add(rmBytes);
        }

        if (!requestBytes.isEmpty()) {

            byte[] totalBytes = new byte[totalLength];

            int position = 0;
            for (byte[] rmBytes : requestBytes) {
                System.arraycopy(rmBytes, 0, totalBytes, position, rmBytes.length);
                position += rmBytes.length;
            }

            return totalBytes;
        }

        throw new IllegalStateException("Request Message is Empty !!!");
    }

    public static ProposeMessage toMessage(int id, long sequence, long key, byte[] totalBytes) {

        // totalBytes内是Request的内容
        int position = 0;

        List<RequestMessage> requestMessages = new ArrayList<>();

        while (position < totalBytes.length) {
            //|length|id|sequence|body|
            byte[] rmLengthBytes = new byte[Integer.BYTES];

            System.arraycopy(totalBytes, position, rmLengthBytes, 0, rmLengthBytes.length);

            position += rmLengthBytes.length;

            byte[] rmBytes = new byte[ByteUtils.bytesToInt(rmLengthBytes)];

            System.arraycopy(totalBytes, position, rmBytes, 0, rmBytes.length);

            position += rmBytes.length;

            requestMessages.add(RequestMessage.toMessage(rmBytes));
        }

        return new ProposeMessage(id, sequence, key, requestMessages);
    }
}
