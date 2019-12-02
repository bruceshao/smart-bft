package com.x.farmer.bft.message;

public interface Message {

    public static final int HEADER_DATA = 0xabcd;

    int id();

    long sequence();

    MessageType type();

    long key();

    byte[] toBytes();
}
