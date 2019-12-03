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
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;
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
            WriteMessage acceptWriteMessage = canAccept(writeMessages);

            if (acceptWriteMessage != null) {
                acceptMessage = convertToAcceptMessage(acceptWriteMessage, listener.getRequestMessages());
            } else {
                throw new IllegalStateException("Can not accept all Write Messages !!!");
            }
        } catch (BftServiceException e) {
            // TODO 已经被处理过，因此不需要再处理
            return;
        } catch (Exception e) {
            // 生成不同意的答案
            byte[] agreement = MessageUtils.randomBytes();

            acceptMessage = new AcceptMessage(viewController.localId(), 0L, listener.getKey(),
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

    private WriteMessage canAccept(List<WriteMessage> writeMessages) {

        // 首先判断数量是否满足
        if (viewController.isMeetRule(writeMessages.size())) {

            // 判断agree消息的数量是否满足数量，
            Map<byte[], Integer> agreements = new HashMap<>(writeMessages.size());
            // 首先对消息进行分组
            for (WriteMessage writeMessage : writeMessages) {
                byte[] agreement = writeMessage.getAgreement();
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
                for (WriteMessage writeMessage : writeMessages) {
                    byte[] agreement = writeMessage.getAgreement();
                    if (!Arrays.equals(agreement, confirmBytes)) {
                        return writeMessage;
                    }
                }
            }

            return null;
        }

        return null;
    }

    private AcceptMessage convertToAcceptMessage(WriteMessage writeMessage, List<RequestMessage> requestMessages) {

        List<byte[]> commands = new ArrayList<>(requestMessages.size());

        for (RequestMessage rm : requestMessages) {

            commands.add(rm.getBody());
        }

        byte[] agreement = messageHandler.execute(commands);

        return new AcceptMessage(viewController.localId(), writeMessage.getSequence(), writeMessage.key(),
                agreement, writeMessage.getIdAndSequences());
    }
}