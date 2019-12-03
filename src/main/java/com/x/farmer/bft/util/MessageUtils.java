package com.x.farmer.bft.util;

import com.x.farmer.bft.message.*;

import java.util.Random;

public class MessageUtils {

    private static final Random RM = new Random();

    private static final int RM_BYTES_LENGTH = 32;

    public static Message decode(byte[] msgBytes) {

        return null;
    }

    public static Message decode(String msgString) {

        return null;
    }

    public static Message decode(int headerData, int id, long sequence, int type, long key, byte[] body) {

        if (headerData != Message.HEADER_DATA) {
            return null;
        }

        if (MessageType.HEARTBEAT.code() == type) {
            // 心跳
            return HeartBeatMessage.toMessage(id, sequence, key, body);
        } else if (MessageType.REQUEST.code() == type) {
            // 客户端请求消息
            return RequestMessage.toMessage(id, sequence, key, body);
        } else if (MessageType.PROPOSE.code() == type) {
            // ProposeMessage消息
            return ProposeMessage.toMessage(id, sequence, key, body);
        } else if (MessageType.WRITE.code() == type) {
            // writeMessage消息
            return WriteMessage.toMessage(id, sequence, key, body);
        } else if (MessageType.ACCEPT.code() == type) {
            // AcceptMessage消息
            return AcceptMessage.toMessage(id, sequence, key, body);
        }

        return null;

    }

    public static long newKey(int id, long sequence) {

        return (System.nanoTime() + sequence) * 1000 + id;
    }

    public static byte[] randomBytes() {
        return randomBytes(RM_BYTES_LENGTH);
    }

    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];

        RM.nextBytes(bytes);

        return bytes;
    }
}
