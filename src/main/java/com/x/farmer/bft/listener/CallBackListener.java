package com.x.farmer.bft.listener;

import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.message.RequestMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CallBackListener<T> {

    private Lock lock = new ReentrantLock();

    private boolean isRead = false;

    private CountDownLatch latch;

    private long key;

    private List<RequestMessage> requestMessages;

    private List<T> responses = new ArrayList<>();

    public CallBackListener(int size, long key) {
        this.latch = new CountDownLatch(size);
        this.key = key;
    }

    public CallBackListener(int size, long key, List<RequestMessage> requestMessages) {
        this.latch = new CountDownLatch(size);
        this.key = key;
        this.requestMessages = requestMessages;
    }

    public void receive(T t) {
        lock.lock();
        try {
            responses.add(t);
            this.latch.countDown();
        } finally {
            lock.unlock();
        }
    }

    public List<T> waitResponses(long timeout, TimeUnit unit) throws InterruptedException {
        if (isRead) {
            throw new BftServiceException("Response has been handled !!!");
        }
        this.latch.await(timeout, unit);
        isRead = true;
        return responses;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public List<RequestMessage> getRequestMessages() {
        return requestMessages;
    }

    public void setRequestMessages(List<RequestMessage> requestMessages) {
        this.requestMessages = requestMessages;
    }
}
