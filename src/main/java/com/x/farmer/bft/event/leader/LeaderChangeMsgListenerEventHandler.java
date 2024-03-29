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

            LeaderChangeMessage acceptLeaderChangeMessage = canAccept(leaderChangeMessages);

            if (acceptLeaderChangeMessage != null) {
                // TODO 处理领导者改变
                /**
                 * 领导者改变时，会发生如下变化
                 * 1、ViewController的Leader需要进行切换；
                 * 2、最新的Propose消息进行校验时，需要按照该消息来处理；
                 * 3、若当前节点是Leader，则通知当前线程进行处理；
                 */
                viewController.newLeader(acceptLeaderChangeMessage.getNewLeader());
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

    private LeaderChangeMessage canAccept(List<LeaderChangeMessage> leaderChangeMessages) {

        // 首先判断LeaderChange的数量是否达标
        // 然后判断LeaderChange类型是否一致（需要依靠msgID）
        // 然后判断LeaderChange选择的newLeader是否一致

        // 首先判断数量是否满足
        if (viewController.isMeetRule(leaderChangeMessages.size())) {

            LeaderChangeMessage acceptLeaderChangeMessage = leaderChangeMessages.get(0);
            int newLeader = acceptLeaderChangeMessage.getNewLeader();
            if (acceptLeaderChangeMessage.getMsgId() == -1L) {
                // 表明此LeaderChange是由Accept消息触发
                boolean isAccept = true;
                for (int i = 1; i < leaderChangeMessages.size(); i++) {
                    LeaderChangeMessage msg = leaderChangeMessages.get(i);
                    if (msg.getMsgId() != -1L || newLeader != msg.getNewLeader()) {
                        isAccept = false;
                        break;
                    }
                }
                if (isAccept) {
                    return acceptLeaderChangeMessage;
                }
            } else {
                // TODO 超时消息触发，要求msgID和sequence必须一致（待定）
            }
        }

        return null;
    }
}