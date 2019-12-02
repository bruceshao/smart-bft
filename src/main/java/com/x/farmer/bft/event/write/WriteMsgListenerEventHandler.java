package com.x.farmer.bft.event.write;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.event.accept.AcceptMsgListenerEventProducer;
import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.execute.MessageHandler;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
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
public class WriteMsgListenerEventHandler implements EventHandler<CallBackListenerEvent<WriteMessage>> {

    private ViewController viewController;

    private CallBackListenerPool listenerPool;

    private ReplicaClientPool replicaClientPool;

    private ConsensusServer consensusServer;

    private MessageHandler messageHandler;

    private AcceptMsgListenerEventProducer acceptMsgListenerProducer;

    public WriteMsgListenerEventHandler(ViewController viewController,
                                        CallBackListenerPool listenerPool,
                                        ReplicaClientPool replicaClientPool,
                                        ConsensusServer consensusServer,
                                        MessageHandler messageHandler,
                                        AcceptMsgListenerEventProducer acceptMsgListenerProducer) {
        this.viewController = viewController;
        this.listenerPool = listenerPool;
        this.replicaClientPool = replicaClientPool;
        this.consensusServer = consensusServer;
        this.messageHandler = messageHandler;
        this.acceptMsgListenerProducer = acceptMsgListenerProducer;
    }

    @Override
    public void onEvent(CallBackListenerEvent<WriteMessage> callBackListenerEvent, long sequence, boolean endOfBatch) throws Exception {
        handleListener(callBackListenerEvent.getCallBackListener());
    }

    private void handleListener(CallBackListener<WriteMessage> listener) {

        AcceptMessage acceptMessage = null;

        try {
            // 假设超时的消息会在大部分节点定时器中自动发现，并发送
            List<WriteMessage> writeMessages = listener.waitResponses(viewController.getBatchTimeout(), TimeUnit.MILLISECONDS);
            // 判断是否满足条件
            boolean canAccept = canAccept(writeMessages);

            if (canAccept) {
                acceptMessage = convertToAcceptMessage(writeMessages.get(0), listener.getRequestMessages());
            }
        } catch (BftServiceException e) {
            // TODO 已经被处理过，因此不需要再处理
            return;
        } catch (Exception e) {
            // 生成不同意的答案
            byte[] agreement = MessageUtils.randomBytes();

            acceptMessage = new AcceptMessage(viewController.localId(), viewController.newSequence(), listener.getKey(),
                    agreement);
        } finally {
            // 不管是否满足都需要将该Listener移除
            consensusServer.removeWriteMsgListener(listener);
        }

        // 生成监听器
        CallBackListener<AcceptMessage> acceptMsgListener = listenerPool.acceptMsgCallBackListener(
                listener.getKey(), listener.getRequestMessages());

        consensusServer.addAcceptMsgListener(acceptMsgListener);

        replicaClientPool.broadcastAcceptMessage(acceptMessage);

        acceptMsgListener.receive(acceptMessage);

        // 通过另外的线程处理该监听器
        acceptMsgListenerProducer.produce(acceptMsgListener);
    }

    private boolean canAccept(List<WriteMessage> writeMessages) {

        // 首先判断数量是否满足
        if (viewController.isMeetRule(writeMessages.size())) {
            // 判断WriteMessage中的内容是否一致
            byte[] agreement = writeMessages.get(0).getAgreement();

            for (int i = 1; i < writeMessages.size(); i++) {

                if (!Arrays.equals(agreement, writeMessages.get(i).getAgreement())) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    private AcceptMessage convertToAcceptMessage(WriteMessage writeMessage, List<RequestMessage> requestMessages) {

        List<byte[]> commands = new ArrayList<>(requestMessages.size());

        for (RequestMessage rm : requestMessages) {

            commands.add(rm.getBody());
        }

        byte[] agreement = messageHandler.execute(commands);

        return new AcceptMessage(viewController.localId(), viewController.newSequence(), writeMessage.key(),
                agreement, writeMessage.getIdAndSequences());
    }
}