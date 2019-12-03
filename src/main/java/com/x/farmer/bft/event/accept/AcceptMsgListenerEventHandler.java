package com.x.farmer.bft.event.accept;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.LayerEngine;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.data.RequestDatas;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventProducer;
import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.execute.MessageHandler;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.AcceptMessage;
import com.x.farmer.bft.message.LeaderChangeMessage;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.WriteMessage;
import com.x.farmer.bft.server.ConsensusServer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件处理器
 *     事件的消费者
 */
public class AcceptMsgListenerEventHandler implements EventHandler<CallBackListenerEvent<AcceptMessage>> {

    private ViewController viewController;

    private CallBackListenerPool listenerPool;

    private ReplicaClientPool replicaClientPool;

    private ConsensusServer consensusServer;

    private MessageHandler messageHandler;

    private RequestDatas requestDatas;

    private LayerEngine layerEngine;

    private LeaderChangeMsgListenerEventProducer listenerEventProducer;

    public AcceptMsgListenerEventHandler(ViewController viewController,
                                         CallBackListenerPool listenerPool,
                                         ReplicaClientPool replicaClientPool,
                                         ConsensusServer consensusServer,
                                         MessageHandler messageHandler,
                                         RequestDatas requestDatas) {
        this.viewController = viewController;
        this.listenerPool = listenerPool;
        this.replicaClientPool = replicaClientPool;
        this.consensusServer = consensusServer;
        this.messageHandler = messageHandler;
        this.requestDatas = requestDatas;
    }

    public void initLayerEngineAndLeaderChangeListener(LayerEngine layerEngine, LeaderChangeMsgListenerEventProducer listenerEventProducer) {
        this.layerEngine = layerEngine;
        this.listenerEventProducer = listenerEventProducer;
    }

    @Override
    public void onEvent(CallBackListenerEvent<AcceptMessage> callBackListenerEvent, long sequence, boolean endOfBatch) throws Exception {
        handleListener(callBackListenerEvent.getCallBackListener());
    }

    private void handleListener(CallBackListener<AcceptMessage> listener) {

        try {
            List<AcceptMessage> acceptMessages = listener.waitResponses(viewController.getBatchTimeout(), TimeUnit.MILLISECONDS);

            AcceptMessage acceptAcceptMessage = canAccept(acceptMessages);
            // 判断是否满足条件
            if (acceptAcceptMessage != null) {
                execute(listener.getRequestMessages(), acceptAcceptMessage);
            } else {
                // 触发LeaderChange消息
                LeaderChangeMessage leaderChangeMessage = convertToLeaderChangeMessage(acceptMessages, listener);

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
            return;
        } catch (Exception e) {
            // 打印即可
            e.printStackTrace();
        } finally {
            // 不管是否满足都需要将该Listener移除
            consensusServer.removeAcceptMsgListener(listener);
            if (viewController.isLeader()) {
                layerEngine.canPropose();
            }
        }
    }

    private LeaderChangeMessage convertToLeaderChangeMessage(List<AcceptMessage> acceptMessages, CallBackListener<AcceptMessage> listener) {
        return new LeaderChangeMessage(viewController.localId(), 0L, listener.getKey(), viewController.newLeader());
    }

    private AcceptMessage canAccept(List<AcceptMessage> acceptMessages) {

        // 首先判断数量是否满足
        if (viewController.isMeetRule(acceptMessages.size())) {

            // 判断agree消息的数量是否满足数量，
            Map<byte[], Integer> agreements = new HashMap<>(acceptMessages.size());
            // 首先对消息进行分组
            for (AcceptMessage acceptMessage : acceptMessages) {
                byte[] agreement = acceptMessage.getAgreement();
                agreements.putIfAbsent(agreement, 1);
                int size = agreements.get(agreement);
                agreements.put(agreement, size + 1);
            }
            byte[] confirmBytes = null;
            for (Map.Entry<byte[], Integer> entry : agreements.entrySet()) {
                byte[] key = entry.getKey();
                int size = entry.getValue();
                if (viewController.isMeetRule(size)) {
                    confirmBytes = key;
                    break;
                }
            }

            if (confirmBytes != null) {
                // 寻找一个满足条件的WriteMessage作为结果
                for (AcceptMessage acceptMessage : acceptMessages) {
                    byte[] agreement = acceptMessage.getAgreement();
                    if (!Arrays.equals(agreement, confirmBytes)) {
                        return acceptMessage;
                    }
                }
            }

            return null;
        }

        return null;
    }

    private void execute(List<RequestMessage> requestMessages, AcceptMessage acceptMessage) {

        List<byte[]> commands = new ArrayList<>(requestMessages.size());

        for (RequestMessage rm : requestMessages) {

            commands.add(rm.getBody());
        }

        messageHandler.execute(commands);

        // 然后将信息移除
        requestDatas.removeRequestMessage(acceptMessage.getIdAndSequences());
    }
}