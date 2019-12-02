package com.x.farmer.bft.server.handler;

import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventProducer;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventProducer;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.*;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventProducer;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEventProducer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.collections.map.LRUMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ChannelHandler.Sharable
public class ConsensusMessageHandler extends ChannelInboundHandlerAdapter {

    private ProposeMessageEventProducer proposeMsgProducer;

    private CallBackListenerPool listenerPool;

    private TimeoutRequestMsgListenerEventProducer timeoutEventProducer;

    private LeaderChangeMsgListenerEventProducer leaderChangeEventProducer;

    private final Lock writeMsgLock = new ReentrantLock();

    private final Lock acceptMsgLock = new ReentrantLock();

    private final Lock leaderChangeMsgLock = new ReentrantLock();

    private final Lock requestTimeoutMsgLock = new ReentrantLock();

    private final LRUMap writeMsgListenerRemoves = new LRUMap(1024);

    private final LRUMap acceptMsgListenerRemoves = new LRUMap(1024);

    private final Map<Long, CallBackListener<WriteMessage>> writeMsgListeners = new ConcurrentHashMap<>();

    private final Map<Long, CallBackListener<RequestTimeoutMessage>> requestTimeoutMsgListeners = new ConcurrentHashMap<>();

    private final Map<Long, CallBackListener<AcceptMessage>> acceptMsgListeners = new ConcurrentHashMap<>();

    private final Map<Long, CallBackListener<LeaderChangeMessage>> leaderChangeMsgListeners = new ConcurrentHashMap<>();

    private final WriteMessageCache writeMessageCache = new WriteMessageCache();

    private final AcceptMessageCache acceptMessageCache = new AcceptMessageCache();

    public ConsensusMessageHandler(CallBackListenerPool listenerPool,
                                   TimeoutRequestMsgListenerEventProducer timeoutEventProducer,
                                   ProposeMessageEventProducer proposeMsgProducer,
                                   LeaderChangeMsgListenerEventProducer leaderChangeEventProducer) {
        this.listenerPool = listenerPool;
        this.timeoutEventProducer = timeoutEventProducer;
        this.proposeMsgProducer = proposeMsgProducer;
        this.leaderChangeEventProducer = leaderChangeEventProducer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 有数据接入
        Message message = (Message) msg;

        if (message != null && message.type() != null) {
            if (message.type() == MessageType.PROPOSE ||
                    message.type() == MessageType.WRITE ||
                    message.type() == MessageType.ACCEPT ||
                    message.type() == MessageType.REQUEST_TIMEOUT ||
                    message.type() == MessageType.LEADER_CHANGE) {
                // 类型不同，处理流程不同
                MessageType type = message.type();
                switch (type) {
                    case PROPOSE:
                        handleProposeMessage((ProposeMessage) message);
                        break;

                    case WRITE:
                        handleWriteMessage((WriteMessage) message);
                        break;

                    case ACCEPT:
                        handleAcceptMessage((AcceptMessage) message);
                        break;

                    case REQUEST_TIMEOUT:
                        handleRequestTimeoutMessage((RequestTimeoutMessage) message);
                        break;

                    case LEADER_CHANGE:
                        handleLeaderChangeMessage((LeaderChangeMessage) message);
                        break;

                    default:
                }
            }
        }
    }

    private void handleLeaderChangeMessage(LeaderChangeMessage message) {
        leaderChangeMsgLock.lock();

        try {
            long msgKey = message.key();
            // 收到超时消息
            CallBackListener<LeaderChangeMessage> listener = leaderChangeMsgListener(msgKey);
            if (listener != null) {
                listener.receive(message);
            } else {
                listener = listenerPool.leaderChangeMsgCallBackListener(msgKey);
                // 后续的消息不会再执行该消息
                addLeaderChangeMsgListener(listener);
                listener.receive(message);
                leaderChangeEventProducer.produce(listener);
            }
        } finally {
            leaderChangeMsgLock.unlock();
        }
    }

    private void handleRequestTimeoutMessage(RequestTimeoutMessage message) {

        requestTimeoutMsgLock.lock();

        try {
            long msgKey = message.key();
            // 收到超时消息
            CallBackListener<RequestTimeoutMessage> listener = requestTimeoutMsgListener(msgKey);
            if (listener != null) {
                listener.receive(message);
            } else {
                listener = listenerPool.requestTimeoutMsgCallBackListener(msgKey);
                // 后续的消息不会再执行该消息
                addRequestTimeoutMsgListener(listener);
                listener.receive(message);
                timeoutEventProducer.produce(listener);
            }
        } finally {
            requestTimeoutMsgLock.unlock();
        }
    }



    private void handleAcceptMessage(AcceptMessage message) {

        acceptMsgLock.lock();

        try {
            long msgKey = message.key();
            // 收到第三阶段的消息
            CallBackListener<AcceptMessage> listener = acceptMsgListener(msgKey);
            if (listener != null) {
                listener.receive(message);
            } else {
                if (!acceptMsgListenerRemoves.containsKey(msgKey)) {
                    acceptMessageCache.addAcceptMessage(message);
                }
            }
        } finally {
            acceptMsgLock.unlock();
        }
    }

    private void handleWriteMessage(WriteMessage message) {

        writeMsgLock.lock();

        try {

            long msgKey = message.getKey();

            // 收到第二阶段的消息
            CallBackListener<WriteMessage> listener = writeMsgListener(msgKey);
            if (listener != null) {
                listener.receive(message);
            } else {
                if (!writeMsgListenerRemoves.containsKey(msgKey)) {
                    // 说明未被移除（处于尚未加入的状态），可以添加到缓存
                    // 缓存，有可能自己本身还没有处理Propose消息
                    writeMessageCache.addWriteMessage(message);
                }
            }
        } finally {
            writeMsgLock.unlock();
        }
    }

    private void handleProposeMessage(ProposeMessage message) {
        proposeMsgProducer.produce(message);
    }


    public void addRequestTimeoutMsgListener(CallBackListener<RequestTimeoutMessage> listener) {

        requestTimeoutMsgLock.lock();

        try {
            requestTimeoutMsgListeners.put(listener.getKey(), listener);
        } finally {
            requestTimeoutMsgLock.unlock();
        }
    }

    public void removeRequestTimeoutMsgListener(CallBackListener<RequestTimeoutMessage> listener) {
        removeRequestTimeoutMsgListener(listener.getKey());
    }

    public void removeRequestTimeoutMsgListener(long key) {

        requestTimeoutMsgLock.lock();

        try {
            requestTimeoutMsgListeners.remove(key);
        } finally {
            requestTimeoutMsgLock.unlock();
        }
    }

    public void addWriteMsgListener(CallBackListener<WriteMessage> listener) {

        writeMsgLock.lock();

        try {
            writeMsgListeners.put(listener.getKey(), listener);
            writeMessageCache.updateListener(listener);
        } finally {
            writeMsgLock.unlock();
        }

    }

    public void removeWriteMsgListener(CallBackListener<WriteMessage> listener) {
        removeWriteMsgListener(listener.getKey());
    }

    public void removeWriteMsgListener(long key) {

        writeMsgLock.lock();

        try {
            // LRU算法临时缓存记录，防止后续记录进入是在缓存中导致内存泄露
            writeMsgListenerRemoves.put(key, "WRITE");
            writeMsgListeners.remove(key);
        } finally {
            writeMsgLock.unlock();
        }
    }

    public void addAcceptMsgListener(CallBackListener<AcceptMessage> listener) {

        acceptMsgLock.lock();

        try {
            acceptMsgListeners.put(listener.getKey(), listener);
            acceptMessageCache.updateListener(listener);
        } finally {
            acceptMsgLock.unlock();
        }

    }

    public void addLeaderChangeMsgListener(CallBackListener<LeaderChangeMessage> listener) {
        leaderChangeMsgLock.lock();
        try {
            leaderChangeMsgListeners.put(listener.getKey(), listener);
        } finally {
            leaderChangeMsgLock.unlock();
        }
    }

    public void removeLeaderChangeMsgListener(CallBackListener<LeaderChangeMessage> listener) {

        removeLeaderChangeMsgListener(listener.getKey());
    }

    public void removeLeaderChangeMsgListener(long key) {

        leaderChangeMsgLock.lock();

        try {
            leaderChangeMsgListeners.remove(key);
        } finally {
            leaderChangeMsgLock.unlock();
        }
    }

    public void removeAcceptMsgListener(CallBackListener<AcceptMessage> listener) {
        removeAcceptMsgListener(listener.getKey());
    }

    public void removeAcceptMsgListener(long key) {

        acceptMsgLock.lock();

        try {
            acceptMsgListenerRemoves.put(key, "ACCEPT");
            acceptMsgListeners.remove(key);
        } finally {
            acceptMsgLock.unlock();
        }
    }

    private CallBackListener<RequestTimeoutMessage> requestTimeoutMsgListener(long key) {
        return requestTimeoutMsgListeners.get(key);
    }

    private CallBackListener<LeaderChangeMessage> leaderChangeMsgListener(long key) {
        return leaderChangeMsgListeners.get(key);
    }

    private CallBackListener<WriteMessage> writeMsgListener(long key) {

        return writeMsgListeners.get(key);
    }

    private CallBackListener<AcceptMessage> acceptMsgListener(long key) {

        return acceptMsgListeners.get(key);
    }


    private static class WriteMessageCache {

        private Map<Long, List<WriteMessage>> writeMessages = new ConcurrentHashMap<>();

        public void addWriteMessage(WriteMessage writeMessage) {
            writeMessages.computeIfAbsent(writeMessage.key(),
                    k -> new ArrayList<>()).add(writeMessage);
        }

        public void updateListener(CallBackListener<WriteMessage> listener) {

            long listenerKey = listener.getKey();

            List<WriteMessage> writeMsgs = writeMessages.get(listenerKey);

            if (writeMsgs != null && !writeMsgs.isEmpty()) {
                for (WriteMessage writeMsg : writeMsgs) {
                    listener.receive(writeMsg);
                }

                // 移除该Key
                writeMessages.remove(listenerKey);
            }
        }

    }

    private static class AcceptMessageCache {

        private Map<Long, List<AcceptMessage>> acceptMessages = new ConcurrentHashMap<>();

        void addAcceptMessage(AcceptMessage acceptMessage) {
            acceptMessages.computeIfAbsent(acceptMessage.key(),
                    k -> new ArrayList<>()).add(acceptMessage);
        }

        void updateListener(CallBackListener<AcceptMessage> listener) {

            long listenerKey = listener.getKey();

            List<AcceptMessage> acceptMsgs = acceptMessages.get(listenerKey);

            if (acceptMsgs != null && !acceptMsgs.isEmpty()) {
                for (AcceptMessage acceptMsg : acceptMsgs) {
                    listener.receive(acceptMsg);
                }

                // 移除该Key
                acceptMessages.remove(listenerKey);
            }
        }

    }
}
