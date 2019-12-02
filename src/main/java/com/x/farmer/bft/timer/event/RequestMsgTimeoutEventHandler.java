package com.x.farmer.bft.timer.event;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventProducer;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.server.ConsensusServer;

/**
 * 事件处理器
 *     事件的消费者
 */
public class RequestMsgTimeoutEventHandler implements EventHandler<RequestMsgTimeoutEvent> {

    private CallBackListenerPool listenerPool;

    private ConsensusServer consensusServer;

    private ReplicaClientPool replicaClientPool;

    private TimeoutRequestMsgListenerEventProducer listenerEventProducer;

    public RequestMsgTimeoutEventHandler(CallBackListenerPool listenerPool,
                                         ConsensusServer consensusServer,
                                         ReplicaClientPool replicaClientPool,
                                         TimeoutRequestMsgListenerEventProducer listenerEventProducer) {
        this.listenerPool = listenerPool;
        this.consensusServer = consensusServer;
        this.replicaClientPool = replicaClientPool;
        this.listenerEventProducer = listenerEventProducer;
    }

    @Override
    public void onEvent(RequestMsgTimeoutEvent requestMsgTimeoutEvent, long sequence, boolean endOfBatch) throws Exception {
        // 处理Propose消息
        RequestTimeoutMessage requestTimeoutMessage = requestMsgTimeoutEvent.getRequestTimeoutMessage();

        // 重新检查一遍，看是否满足超时条件
        if (check(requestTimeoutMessage)) {

            // 生成回调监听器
            CallBackListener<RequestTimeoutMessage> listener = listenerPool.requestTimeoutMsgCallBackListener(
                    requestTimeoutMessage.key());

            consensusServer.addRequestTimeoutMsgListener(listener);

            replicaClientPool.broadcastTimeoutMessage(requestTimeoutMessage);

            listener.receive(requestTimeoutMessage);

            listenerEventProducer.produce(listener);
        }
    }

    private boolean check(RequestTimeoutMessage requestMessage) {
        return requestMessage.getState() < RequestMessage.STATE_PROPOSED;
    }
}