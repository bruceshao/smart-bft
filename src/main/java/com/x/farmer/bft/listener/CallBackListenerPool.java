package com.x.farmer.bft.listener;

import com.x.farmer.bft.message.*;
import org.apache.commons.collections.map.LRUMap;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CallBackListenerPool {

    private static final int MAP_MAX_SIZE = 1024;

    private final Lock timeoutListenerLock = new ReentrantLock();

    private final Lock leaderChangeListenerLock = new ReentrantLock();

    private final Lock writeListenerLock = new ReentrantLock();

    private final Lock acceptListenerLock = new ReentrantLock();

    private final LRUMap timeoutListenerMap = new LRUMap(MAP_MAX_SIZE);

    private final LRUMap leaderChangeListenerMap = new LRUMap(MAP_MAX_SIZE);

    private final LRUMap writeListenerMap = new LRUMap(MAP_MAX_SIZE);

    private final LRUMap acceptListenerMap = new LRUMap(MAP_MAX_SIZE);

    private final int size;

    public CallBackListenerPool(int size) {
        this.size = size;
    }

    public CallBackListener<LeaderChangeMessage> leaderChangeMsgCallBackListener(long key) {
        Object listener = leaderChangeListenerMap.get(key);

        if (listener == null) {
            leaderChangeListenerLock.lock();
            try {
                listener = leaderChangeListenerMap.get(key);
                if (listener == null) {
                    listener = new CallBackListener<>(size, key);
                    leaderChangeListenerMap.put(key, listener);
                }
            } finally {
                leaderChangeListenerLock.unlock();
            }
        }

        return (CallBackListener<LeaderChangeMessage>) listener;
    }

    public CallBackListener<RequestTimeoutMessage> requestTimeoutMsgCallBackListener(long key) {

        Object listener = timeoutListenerMap.get(key);

        if (listener == null) {
            timeoutListenerLock.lock();
            try {
                listener = timeoutListenerMap.get(key);
                if (listener == null) {
                    listener = new CallBackListener<>(size, key);
                    timeoutListenerMap.put(key, listener);
                }
            } finally {
                timeoutListenerLock.unlock();
            }
        }

        return (CallBackListener<RequestTimeoutMessage>) listener;
    }


    public CallBackListener<WriteMessage> writeMsgCallBackListener(long key, List<RequestMessage> requestMessages) {

        Object listener = writeListenerMap.get(key);

        if (listener == null) {
            writeListenerLock.lock();
            try {
                listener = writeListenerMap.get(key);
                if (listener == null) {
                    listener = new CallBackListener<>(size, key, requestMessages);
                    writeListenerMap.put(key, listener);
                }
            } finally {
                writeListenerLock.unlock();
            }
        }

        return (CallBackListener<WriteMessage>) listener;
    }

    public CallBackListener<AcceptMessage> acceptMsgCallBackListener(long key, List<RequestMessage> requestMessages) {

        Object listener = acceptListenerMap.get(key);

        if (listener == null) {
            acceptListenerLock.lock();
            try {
                listener = acceptListenerMap.get(key);
                if (listener == null) {
                    listener = new CallBackListener<>(size, key, requestMessages);
                    acceptListenerMap.put(key, listener);
                }
            } finally {
                acceptListenerLock.unlock();
            }
        }


        return (CallBackListener<AcceptMessage>) listener;
    }
}
