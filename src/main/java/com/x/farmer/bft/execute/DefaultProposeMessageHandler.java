package com.x.farmer.bft.execute;

import com.x.farmer.bft.util.ByteUtils;

import java.util.List;

public class DefaultProposeMessageHandler implements MessageHandler {

    /**
     * 默认Propose消息处理器
     *
     * @param messages
     * @return
     *     返回size对应的字节数组
     */
    @Override
    public byte[] execute(List<byte[]> messages) {

        return ByteUtils.intToBytes(messages.size());
    }
}
