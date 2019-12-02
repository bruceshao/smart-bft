package com.x.farmer.bft.server;

import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventProducer;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventProducer;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.AcceptMessage;
import com.x.farmer.bft.message.LeaderChangeMessage;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.message.WriteMessage;
import com.x.farmer.bft.server.codec.BftMessageDecoder;
import com.x.farmer.bft.server.codec.BftMessageEncoder;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventProducer;
import com.x.farmer.bft.server.handler.ConsensusMessageHandler;
import com.x.farmer.bft.server.handler.HeartBeatReceiverHandler;
import com.x.farmer.bft.server.handler.HeartBeatReceiverTrigger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ConsensusServer extends AbstractServer {

    /**
     * 消息接收Handler
     */
    private ConsensusMessageHandler consensusMessageHandler;

    public ConsensusServer(ViewController viewController) {
        this.port = viewController.getLocal().getBftPort();
        this.localId = viewController.localId();
    }

    /**
     * 初始化MessageHandler
     *
     * @param listenerPool
     * @param timeoutEventProducer
     * @param proposeMsgProducer
     * @param leaderChangeEventProducer
     */
    public void init(CallBackListenerPool listenerPool,
                     TimeoutRequestMsgListenerEventProducer timeoutEventProducer,
                     ProposeMessageEventProducer proposeMsgProducer,
                     LeaderChangeMsgListenerEventProducer leaderChangeEventProducer) {
        this.consensusMessageHandler = new ConsensusMessageHandler(
                listenerPool, timeoutEventProducer, proposeMsgProducer, leaderChangeEventProducer);
    }

    /**
     * 添加Write消息回调监听器
     *
     * @param listener
     */
    public void addWriteMsgListener(CallBackListener<WriteMessage> listener) {
        consensusMessageHandler.addWriteMsgListener(listener);
    }

    /**
     * 添加Accept消息回调监听器
     *
     * @param listener
     */
    public void addAcceptMsgListener(CallBackListener<AcceptMessage> listener) {
        consensusMessageHandler.addAcceptMsgListener(listener);
    }

    /**
     * 添加Request超时消息回调监听器
     *
     * @param listener
     */
    public void addRequestTimeoutMsgListener(CallBackListener<RequestTimeoutMessage> listener) {
        consensusMessageHandler.addRequestTimeoutMsgListener(listener);
    }

    /**
     * 添加LeaderChange消息回调监听器
     *
     * @param listener
     */
    public void addLeaderChangeMsgListener(CallBackListener<LeaderChangeMessage> listener) {
        consensusMessageHandler.addLeaderChangeMsgListener(listener);
    }


    /**
     * 移除Write应答处理监听器
     *
     * @param listener
     */
    public void removeWriteMsgListener(CallBackListener<WriteMessage> listener) {
        consensusMessageHandler.removeWriteMsgListener(listener);
    }

    /**
     * 移除Accept应答处理监听器
     *
     * @param listener
     */
    public void removeAcceptMsgListener(CallBackListener<AcceptMessage> listener) {
        consensusMessageHandler.removeAcceptMsgListener(listener);
    }

    /**
     * 移除Request超时应答处理监听器
     *
     * @param listener
     */
    public void removeRequestTimeoutMsgListener(CallBackListener<RequestTimeoutMessage> listener) {
        consensusMessageHandler.removeRequestTimeoutMsgListener(listener);
    }

    /**
     * 移除领导者改变处理监听器
     *
     * @param listener
     */
    public void removeLeaderChangeMsgListener(CallBackListener<LeaderChangeMessage> listener) {
        consensusMessageHandler.removeLeaderChangeMsgListener(listener);
    }

    @Override
    protected String nameFormat() {
        return "consensus-receivers-%d";
    }

    @Override
    protected void initBootstrap(ServerBootstrap bootstrap) {
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024 * 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .localAddress(new InetSocketAddress(this.port))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline()
                                .addLast(new BftMessageDecoder(M, 28, 4))
                                .addLast(new BftMessageEncoder())
                                .addLast(new IdleStateHandler(8, 0, 0, TimeUnit.SECONDS))
                                .addLast(new HeartBeatReceiverTrigger(getLocalId()))
                                .addLast(new HeartBeatReceiverHandler(getLocalId()))
                                .addLast(consensusMessageHandler);
                    }
                });
    }
}
