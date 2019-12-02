package com.x.farmer.bft.timer;

public class RequestMessageRemark {

    private int id;

    private long sequence;

    private long time;

    public RequestMessageRemark(int id, long sequence) {
        this.id = id;
        this.sequence = sequence;
        this.time = System.currentTimeMillis();
    }

    public int getId() {
        return id;
    }

    public long getSequence() {
        return sequence;
    }

    public long getTime() {
        return time;
    }
}
