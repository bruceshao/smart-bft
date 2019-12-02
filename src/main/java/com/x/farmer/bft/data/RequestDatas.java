package com.x.farmer.bft.data;

import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.timer.RequestMessageTimer;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestDatas {

    private final Lock requestLock = new ReentrantLock();

    private final Map<Integer, TreeMap<Long, RequestMessage>> requestDatas = new ConcurrentHashMap<>();

    private RequestMessageTimer timer;

    public void addRequest(RequestMessage requestMessage) {
        int clientId = requestMessage.id();
        long sequence = requestMessage.sequence();
        timer.addTimer(clientId, sequence);
        requestLock.lock();
        try {
            requestDatas.computeIfAbsent(clientId, k -> new TreeMap<>()).put(sequence, requestMessage);
        } finally {
            requestLock.unlock();
        }
    }

    public void initTimer(RequestMessageTimer timer) {
        this.timer = timer;
    }

    public boolean haveEnoughRequest() {
        return !requestDatas.isEmpty();
    }

    public RequestMessage read(int id, long sequence) {
        // 处理过程不严格要求，不需要加锁
        TreeMap<Long, RequestMessage> tree = requestDatas.get(id);

        if (tree != null) {
            return tree.get(sequence);
        }
        return null;
    }

    public List<RequestMessage> readCanProposeRequestsAndSetState(int size, int state) {

        // TODO 暂时按照ID来处理
        List<RequestMessage> requestMessages = new ArrayList<>();

        requestLock.lock();

        try {
            for (Map.Entry<Integer, TreeMap<Long, RequestMessage>> entry : requestDatas.entrySet()) {

                TreeMap<Long, RequestMessage> entryTree = entry.getValue();

                if (!MapUtils.isEmpty(entryTree)) {

                    for (Map.Entry<Long, RequestMessage> rmEntry : entryTree.entrySet()) {
                        RequestMessage rm = rmEntry.getValue();

                        if (rm.getState() < state) {
                            requestMessages.add(rm.updateState(state));
                            if (requestMessages.size() == size) {
                                return requestMessages;
                            }
                        }
                    }
                }
            }
        } finally {
            requestLock.unlock();
        }

        return requestMessages;
    }

    public void removeRequestMessage(Map<Integer, List<Long>> idAndSequences) {

        requestLock.lock();

        try {
            for (Map.Entry<Integer, List<Long>> idAndSeq : idAndSequences.entrySet()) {

                int id = idAndSeq.getKey();

                List<Long> sequences = idAndSeq.getValue();

                TreeMap<Long, RequestMessage> treeMap = requestDatas.get(id);

                if (treeMap != null && !treeMap.isEmpty() && sequences != null && !sequences.isEmpty()) {
                    for(Long seq : sequences) {
                        treeMap.remove(seq);
                    }
                }
            }
        } finally {
            requestLock.unlock();
        }
    }
}
