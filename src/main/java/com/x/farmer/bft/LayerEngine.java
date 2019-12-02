package com.x.farmer.bft;

import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.data.RequestDatas;
import com.x.farmer.bft.event.write.WriteMsgListenerEventProducer;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.ProposeMessage;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.WriteMessage;
import com.x.farmer.bft.server.ConsensusServer;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventProducer;
import com.x.farmer.bft.service.ProposeMessageHandler;
import com.x.farmer.bft.util.MessageUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LayerEngine implements Runnable, AutoCloseable {

    private boolean work = true;

    private boolean isConsensusWorking = false;

    private Lock leadLock = new ReentrantLock();

    private Condition iAmLeader = leadLock.newCondition();

    private Lock proposeLock = new ReentrantLock();

    private Condition canPropose = proposeLock.newCondition();

    private final RequestDatas requestDatas;

    private final ViewController viewController;

    private ReplicaClientPool replicaClientPool;

    private ConsensusServer consensusServer;

//    private RequestMsgTimeoutEventProducer producer;

    private ProposeMessageEventProducer proposeProducer;

    private ProposeMessageHandler proposeMessageHandler;

    public LayerEngine(RequestDatas requestDatas,
                       ViewController viewController,
                       ReplicaClientPool replicaClientPool,
                       ConsensusServer consensusServer,
                       ProposeMessageEventProducer proposeProducer,
                       ProposeMessageHandler proposeMessageHandler) {
        this.requestDatas = requestDatas;
        this.viewController = viewController;
        this.replicaClientPool = replicaClientPool;
        this.consensusServer = consensusServer;
        this.proposeProducer = proposeProducer;
        this.proposeMessageHandler = proposeMessageHandler;
    }

    public void canPropose() {
        proposeLock.lock();
        try {
            canConsensusWork();
            canPropose.signalAll();
        } finally {
            proposeLock.unlock();
        }
    }

    public void isLeader() {
        leadLock.lock();
        try {
            iAmLeader.signalAll();
        } finally {
            leadLock.unlock();
        }
    }

    @Override
    public void run() {
        while (work) {

            try {

                // 阻塞，等待当前节点是Leader
                leadLock.lock();
                try {
                    if (!viewController.isLeader()) {
                        // 等待成为Leader
                        iAmLeader.awaitUninterruptibly();
                    }
                } finally {
                    leadLock.unlock();
                }

                // 重新判断是否使Leader
                if (!viewController.isLeader()) {
                    // 因为异常导致被终端，则重新来一轮循环
                    continue;
                }

                // 当前节点是Leader，需要判断上一轮是否处理完成
                proposeLock.lock();
                try {
                    if (isConsensusWorking()) {
                        canPropose.awaitUninterruptibly();
                    }
                } finally {
                    proposeLock.unlock();
                }

                // 判断请求是否满足
                if (!requestDatas.haveEnoughRequest()) {
                    continue;
                }

                List<RequestMessage> requestMessages = requestDatas.readCanProposeRequestsAndSetState(
                        viewController.getMaxBatchSize(), RequestMessage.STATE_PROPOSED);

                if (!CollectionUtils.isEmpty(requestMessages)) {
                    // 将请求处理为ProposeMessage
                    consensusWorking();
                    broadcast(proposeMessage(requestMessages));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isConsensusWorking() {
        return isConsensusWorking;
    }

    private void consensusWorking() {
        isConsensusWorking = true;
    }

    private void canConsensusWork() {
        isConsensusWorking = false;
    }

    private void broadcast(ProposeMessage proposeMessage) {

        replicaClientPool.broadcastProposeMessage(proposeMessage);

        proposeProducer.produce(proposeMessage);
    }

    private ProposeMessage proposeMessage(List<RequestMessage> requestMessages) {

        int id = viewController.localId();

        long sequence = viewController.newSequence();

        return new ProposeMessage(id, sequence, MessageUtils.newKey(id, sequence), requestMessages);
    }

    @Override
    public void close() throws Exception {
        work = false;
    }
}
