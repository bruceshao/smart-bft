package com.x.farmer.bft.message;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractMessage implements Message {

    protected Lock lock = new ReentrantLock();

    protected int id;

    protected long sequence;

    protected long key = 0L;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    @Override
    public int id() {
        return this.id;
    }

    @Override
    public long sequence() {
        return this.sequence;
    }

    @Override
    public long key() {
        return this.key;
    }
}
