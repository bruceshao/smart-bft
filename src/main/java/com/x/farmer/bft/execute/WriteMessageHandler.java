package com.x.farmer.bft.execute;

import com.x.farmer.bft.util.ByteUtils;

import java.util.List;

public class WriteMessageHandler implements MessageHandler {
    @Override
    public byte[] execute(List<byte[]> messages) {
//        System.out.println("WriteMessageHandler do nothing !!!");
        return ByteUtils.intToBytes(messages.size());
    }
}
