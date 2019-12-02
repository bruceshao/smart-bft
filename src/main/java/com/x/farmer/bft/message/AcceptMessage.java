package com.x.farmer.bft.message;

import java.util.List;
import java.util.Map;

public class AcceptMessage extends WriteMessage {

    public AcceptMessage(int id, long sequence, long key, byte[] agreement) {
        super(id, sequence, key, agreement);
    }

    public AcceptMessage(int id, long sequence, long key, byte[] agreement, Map<Integer, List<Long>> idAndSequences) {
        super(id, sequence, key, agreement, idAndSequences);
    }

    @Override
    public MessageType type() {
        return MessageType.ACCEPT;
    }

    public static AcceptMessage toMessage(int id, long sequence, long key, byte[] totalBytes) {

        WriteMessage writeMessage = WriteMessage.toMessage(id, sequence, key, totalBytes);

        return new AcceptMessage(id, sequence, key, writeMessage.getAgreement(), writeMessage.getIdAndSequences());
    }
}
