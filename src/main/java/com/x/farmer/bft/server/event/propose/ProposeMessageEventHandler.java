package com.x.farmer.bft.server.event.propose;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.event.write.WriteMsgListenerEventProducer;
import com.x.farmer.bft.execute.DefaultProposeMessageHandler;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.ProposeMessage;
import com.x.farmer.bft.message.WriteMessage;
import com.x.farmer.bft.server.ConsensusServer;
import com.x.farmer.bft.service.ProposeMessageHandler;
import com.x.farmer.bft.util.MessageUtils;

/**
 * 事件处理器
 *     事件的消费者
 */
public class ProposeMessageEventHandler implements EventHandler<ProposeMessageEvent> {

    private CallBackListenerPool listenerPool;

    private ViewController viewController;

    private ProposeMessageHandler proposeMessageHandler;

    private ConsensusServer consensusServer;

    private ReplicaClientPool replicaClientPool;

    private WriteMsgListenerEventProducer writeMsgListenerProducer;

    public ProposeMessageEventHandler(ViewController viewController,
                                      CallBackListenerPool listenerPool,
                                      ProposeMessageHandler proposeMessageHandler,
                                      ConsensusServer consensusServer,
                                      ReplicaClientPool replicaClientPool,
                                      WriteMsgListenerEventProducer writeMsgListenerProducer) {
        this.viewController = viewController;
        this.listenerPool = listenerPool;
        this.proposeMessageHandler = proposeMessageHandler;
        this.consensusServer = consensusServer;
        this.replicaClientPool = replicaClientPool;
        this.writeMsgListenerProducer = writeMsgListenerProducer;
    }

    @Override
    public void onEvent(ProposeMessageEvent proposeMessageEvent, long sequence, boolean endOfBatch) throws Exception {
        // 处理Propose消息
        ProposeMessage proposeMessage = proposeMessageEvent.getProposeMessage();

        WriteMessage writeMessage;
        try {
            // 首先校验该消息是否合法，并生成对应的WriteMessage
            writeMessage = proposeMessageHandler.handle(proposeMessage, true);

        } catch (Exception e) {

            // 有异常的情况下，发送不合法的agreement
            byte[] agreement = MessageUtils.randomBytes();

            writeMessage = new WriteMessage(viewController.getLocal().getId(),
                    proposeMessage.sequence(), proposeMessage.key(), agreement);
        }
        // 生成回调监听器
        CallBackListener<WriteMessage> writeMsgListener = listenerPool.writeMsgCallBackListener(
                proposeMessage.key(), proposeMessage.getRequestMessages());

        // 添加至ConsensusServer
        consensusServer.addWriteMsgListener(writeMsgListener);

        replicaClientPool.broadcastWriteMessage(writeMessage);

        // 本地节点处理
        writeMsgListener.receive(writeMessage);

        // 通过另外的线程处理该监听器
        writeMsgListenerProducer.produce(writeMsgListener);
    }
}