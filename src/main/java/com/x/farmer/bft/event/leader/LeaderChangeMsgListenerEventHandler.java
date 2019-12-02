package com.x.farmer.bft.event.leader;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.LayerEngine;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.event.accept.AcceptMsgListenerEventProducer;
import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.execute.MessageHandler;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.*;
import com.x.farmer.bft.server.ConsensusServer;
import com.x.farmer.bft.util.MessageUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 事件处理器
 *     事件的消费者
 */
public class LeaderChangeMsgListenerEventHandler implements EventHandler<CallBackListenerEvent<LeaderChangeMessage>> {

    private ViewController viewController;

    private ConsensusServer consensusServer;

    private LayerEngine layerEngine;

    public LeaderChangeMsgListenerEventHandler(ViewController viewController,
                                               ConsensusServer consensusServer) {
        this.viewController = viewController;
        this.consensusServer = consensusServer;
    }

    public void initLayerEngine(LayerEngine layerEngine) {
        this.layerEngine = layerEngine;
    }

    @Override
    public void onEvent(CallBackListenerEvent<LeaderChangeMessage> callBackListenerEvent, long sequence, boolean endOfBatch) throws Exception {

        handleListener(callBackListenerEvent.getCallBackListener());
    }

    private void handleListener(CallBackListener<LeaderChangeMessage> listener) {

        List<LeaderChangeMessage> leaderChangeMessages;

        try {
            // 假设超时的消息会在大部分节点定时器中自动发现，并发送
            leaderChangeMessages = listener.waitResponses(viewController.getBatchTimeout(), TimeUnit.MILLISECONDS);

            int newLeader = canAccept(leaderChangeMessages);

            if (newLeader >= 0) {
                // TODO 处理领导者改变
                /**
                 * 领导者改变时，会发生如下变化
                 * 1、ViewController的Leader需要进行切换；
                 * 2、最新的Propose消息进行校验时，需要按照该消息来处理；
                 * 3、若当前节点是Leader，则通知当前线程进行处理；
                 */
                viewController.newLeader(newLeader);
                if (viewController.isLeader()) {
                    layerEngine.isLeader();
                }
            }
        } catch (BftServiceException e) {
            // TODO 已经被处理过，因此不需要再处理
            return;
        } catch (Exception e) {
            // TODO 暂不处理
        } finally {
            consensusServer.removeLeaderChangeMsgListener(listener);
        }
    }

    private int canAccept(List<LeaderChangeMessage> leaderChangeMessages) {

        // 首先判断数量是否满足
        if (viewController.isMeetRule(leaderChangeMessages.size())) {
            // 判断WriteMessage中的内容是否一致
            int newLeader = leaderChangeMessages.get(0).getNewLeader();

            for (int i = 1; i < leaderChangeMessages.size(); i++) {

                if (newLeader != leaderChangeMessages.get(i).getNewLeader()) {
                    return -1;
                }
            }

            return newLeader;
        }

        return -1;
    }
}