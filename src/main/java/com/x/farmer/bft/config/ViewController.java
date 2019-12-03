package com.x.farmer.bft.config;

import com.x.farmer.bft.replica.Node;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 视图控制器
 *
 */
public class ViewController {

    private AtomicLong sequence = new AtomicLong();

    private int leader;

    private Node local;

    private List<Node> remotes;

    private int maxBatchSize;

    private long batchTimeout = 0L;

    private int totalSize;

    public ViewController(Node local) {
        this.local = local;
    }

    public ViewController(int leader, Node local, List<Node> remotes, int maxBatchSize, long batchTimeout) {
        this.leader = leader;
        this.local = local;
        this.remotes = remotes;
        this.maxBatchSize = maxBatchSize;
        this.batchTimeout = batchTimeout;
        this.totalSize = this.remotes.size() + 1;
    }

    public boolean isLeader() {
        return leader == localId();
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public Node getLocal() {
        return local;
    }

    public void setLocal(Node local) {
        this.local = local;
    }

    public List<Node> getRemotes() {
        return remotes;
    }

    public void setRemotes(List<Node> remotes) {
        this.remotes = remotes;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public long getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(long batchTimeout) {
        this.batchTimeout = batchTimeout;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    public long newSequence() {
        return sequence.incrementAndGet();
    }

    public boolean isLeaderSequence(long seq) {
        // 期望发送来的Sequence是比当前值大1
        return sequence.compareAndSet(seq - 1, seq);
    }

    public int localId() {
        return local.getId();
    }

    public boolean isMeetRule(int size) {
        // 3f+1的规则
        int fail = totalSize - size;
        if (fail <= 0) {
            return true;
        }
        return 3 * fail + 1 <= totalSize;
    }

    public int newLeader() {
        return (leader + 1) % totalSize;
    }

    public void newLeader(int leader) {
        this.leader = leader;
        this.sequence.set(0);
    }
}
