package com.x.farmer.bft.event.request;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventProducer;
import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.*;
import com.x.farmer.bft.server.ConsensusServer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 事件处理器
 *     事件的消费者
 */
public class TimeoutRequestMsgListenerEventHandler implements EventHandler<CallBackListenerEvent<RequestTimeoutMessage>> {

    private static final int TIME_OUT_WAIT_TIMES = 10000; // 10秒

    private ViewController viewController;

    private CallBackListenerPool listenerPool;

    private ReplicaClientPool replicaClientPool;

    private ConsensusServer consensusServer;

    private LeaderChangeMsgListenerEventProducer listenerEventProducer;

    public TimeoutRequestMsgListenerEventHandler(ViewController viewController,
                                                 CallBackListenerPool listenerPool,
                                                 ReplicaClientPool replicaClientPool,
                                                 ConsensusServer consensusServer,
                                                 LeaderChangeMsgListenerEventProducer listenerEventProducer) {
        this.viewController = viewController;
        this.listenerPool = listenerPool;
        this.replicaClientPool = replicaClientPool;
        this.consensusServer = consensusServer;
        this.listenerEventProducer = listenerEventProducer;
    }

    @Override
    public void onEvent(CallBackListenerEvent<RequestTimeoutMessage> callBackListenerEvent, long sequence, boolean endOfBatch) throws Exception {
        handleListener(callBackListenerEvent.getCallBackListener());
    }

    private void handleListener(CallBackListener<RequestTimeoutMessage> listener) {

        List<RequestTimeoutMessage> timeoutMessages;

        try {
            // 假设超时的消息会在大部分节点定时器中自动发现，并发送
            timeoutMessages = listener.waitResponses(TIME_OUT_WAIT_TIMES, TimeUnit.MILLISECONDS);
            if (canAccept(timeoutMessages)) {
                // 发送LeaderChangeMessage进行领导者改变处理
                // LeaderChange的Key是超时消息的key
                LeaderChangeMessage leaderChangeMessage = convertToLeaderChangeMessage(timeoutMessages.get(0));

                CallBackListener<LeaderChangeMessage> leaderChangeMsgListener = listenerPool.leaderChangeMsgCallBackListener(
                        listener.getKey());

                if (leaderChangeMsgListener != null) {
                    consensusServer.addLeaderChangeMsgListener(leaderChangeMsgListener);

                    replicaClientPool.broadcastLeaderChangeMessage(leaderChangeMessage);

                    leaderChangeMsgListener.receive(leaderChangeMessage);

                    // 通过另外的线程处理该监听器
                    listenerEventProducer.produce(leaderChangeMsgListener);
                }
            }
        } catch (BftServiceException e) {
            // TODO 已经被处理过，因此不需要再处理
            return;
        } catch (Exception e) {
            // TODO 暂不处理
        } finally {
            consensusServer.removeRequestTimeoutMsgListener(listener);
        }
    }

    private boolean canAccept(List<RequestTimeoutMessage> timeoutMessages) {
        // 判断数量是否满足
        return viewController.isMeetRule(timeoutMessages.size());
    }

    private LeaderChangeMessage convertToLeaderChangeMessage(RequestTimeoutMessage timeoutMessage) {

        return new LeaderChangeMessage(viewController.localId(), timeoutMessage.id(), timeoutMessage.sequence(), timeoutMessage.key(), viewController.newLeader());
    }
}