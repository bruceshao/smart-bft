package com.x.farmer.bft.client.replica;

import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.*;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicaClientPool implements AutoCloseable {

    private final ExecutorService threadPool;

    private List<ReplicaClient> replicaClients;

    public ReplicaClientPool(List<ReplicaClient> replicaClients) {
        this.replicaClients = replicaClients;
        this.threadPool = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 2,
                new BasicThreadFactory.Builder().namingPattern("replica-schedule-pool-%d").daemon(true).build());
    }

    public void connect() {
        for (ReplicaClient client : replicaClients) {
            client.connect();
        }
    }

    public void broadcastProposeMessage(final ProposeMessage proposeMessage) {

        if (proposeMessage == null) {
            throw new IllegalStateException("Propose Message is NULL !!!");
        }
        broadcastMessage(proposeMessage);
    }

    public void broadcastWriteMessage(final WriteMessage writeMessage) {
        if (writeMessage == null) {
            throw new IllegalStateException("Write Message is NULL !!!");
        }
        broadcastMessage(writeMessage);
    }

    public void broadcastAcceptMessage(final AcceptMessage acceptMessage) {

        if (acceptMessage == null) {
            throw new IllegalStateException("Accept Message is NULL !!!");
        }

        broadcastMessage(acceptMessage);
    }

    public void broadcastTimeoutMessage(final RequestTimeoutMessage timeoutMessage) {

        if (timeoutMessage == null) {
            throw new IllegalStateException("Timeout Request Message is NULL !!!");
        }

        broadcastMessage(timeoutMessage);
    }

    public void broadcastLeaderChangeMessage(final LeaderChangeMessage leaderChangeMessage) {

        if (leaderChangeMessage == null) {
            throw new IllegalStateException("Leader Change Message is NULL !!!");
        }

        broadcastMessage(leaderChangeMessage);
    }

    private void broadcastMessage(final Message message) {

        // 发送至其他节点
        for (ReplicaClient replicaClient : replicaClients) {
            threadPool.execute(() -> {
                replicaClient.send(message);
            });
        }
    }

    @Override
    public void close() throws Exception {
        for (ReplicaClient client : replicaClients) {
            client.close();
        }
    }
}
