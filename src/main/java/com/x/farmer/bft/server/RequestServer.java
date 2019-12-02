package com.x.farmer.bft.server;

import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.server.codec.BftMessageDecoder;
import com.x.farmer.bft.server.codec.BftMessageEncoder;
import com.x.farmer.bft.server.event.request.RequestMessageEventProducer;
import com.x.farmer.bft.server.handler.HeartBeatReceiverHandler;
import com.x.farmer.bft.server.handler.HeartBeatReceiverTrigger;
import com.x.farmer.bft.server.handler.RequestMessageHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class RequestServer extends AbstractServer {

    /**
     * 监听端口
     */
    private final int port;

    /**
     * Netty中的BOSS线程
     */
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();

    /**
     * Netty中的Worker线程
     */
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    /**
     * 消息接收Handler
     */
    private RequestMessageHandler requestMessageHandler;

    public RequestServer(ViewController viewController, RequestMessageEventProducer producer) {
        this.port = viewController.getLocal().getMsgPort();
        this.localId = viewController.localId();
        this.requestMessageHandler = new RequestMessageHandler(localId, producer);
    }

    @Override
    protected String nameFormat() {
        return "request-receivers-%d";
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
//                                .addLast(new LoggingHandler(LogLevel.ERROR))
                                .addLast(new IdleStateHandler(8, 0, 0, TimeUnit.SECONDS))
                                .addLast(new BftMessageDecoder(M, 28, 4))
                                .addLast(new BftMessageEncoder())
                                .addLast(new HeartBeatReceiverTrigger(getLocalId()))
                                .addLast(new HeartBeatReceiverHandler(getLocalId()))
                                .addLast(requestMessageHandler);
                    }
                });
    }
}
