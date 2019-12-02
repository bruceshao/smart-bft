package com.x.farmer.bft.client.replica;

import com.x.farmer.bft.client.replica.handler.HeartBeatSenderHandler;
import com.x.farmer.bft.client.replica.handler.HeartBeatSenderTrigger;
import com.x.farmer.bft.client.replica.handler.ReplicaSendHandler;
import com.x.farmer.bft.client.replica.handler.WatchDogHandler;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.message.Message;
import com.x.farmer.bft.replica.Node;
import com.x.farmer.bft.server.codec.BftMessageDecoder;
import com.x.farmer.bft.server.codec.BftMessageEncoder;
import com.x.farmer.bft.util.ThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReplicaClient implements AutoCloseable {

    private final EventLoopGroup loopGroup = new NioEventLoopGroup();

    private static final int M = 1024 * 1024 * 1024;

    private Bootstrap bootstrap;

    private ChannelFuture channelFuture;

    private ViewController viewController;

    private Node remote;

    private boolean isBft = true;

    /**
     * 监听Handler（重连Handler）
     */
    private WatchDogHandler watchDogHandler;

    public ReplicaClient(ViewController viewController, Node remote) {
        this.viewController = viewController;
        init(remote);
    }

    public ReplicaClient(ViewController viewController, Node remote, boolean isBft) {
        this.viewController = viewController;
        this.isBft = isBft;
        init(remote);
    }

    /**
     * 连接
     */
    public void connect() {
        watchDogHandler = new WatchDogHandler(
                viewController.localId(), this.remote.getHost(),
                isBft ? this.remote.getBftPort() : this.remote.getMsgPort(),
                bootstrap);

        ChannelHandlers frontChannelHandlers = new ChannelHandlers();
//                .addHandler(watchDogHandler);

        ChannelHandlers afterChannelHandlers = new ChannelHandlers()
//                .addHandler(new LoggingHandler(LogLevel.ERROR))
//                .addHandler(new BftMessageDecoder(M, 28, 4))
//                .addHandler(new BftMessageEncoder())
                .addHandler(new HeartBeatSenderTrigger(viewController.localId()))
                .addHandler(new HeartBeatSenderHandler())
                .addHandler(watchDogHandler)
                .addHandler(new ReplicaSendHandler())
                ;

        // 初始化watchDogHandler
        watchDogHandler.init(frontChannelHandlers.toArray(), afterChannelHandlers.toArray());

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
//                        .addLast(new LoggingHandler(LogLevel.ERROR))
                        .addLast(new BftMessageDecoder(M, 28, 4))
                        .addLast(new BftMessageEncoder())
                        .addLast(new IdleStateHandler(0, 4, 0, TimeUnit.SECONDS))
                        .addLast(afterChannelHandlers.toArray())
                        ;
//                        .addLast(new HeartBeatSenderTrigger(viewController.localId()))
//                        .addLast(new HeartBeatSenderHandler())
//                        .addLast(watchDogHandler)
//                        .addLast();
//                        .addLast(frontChannelHandlers.toArray())
//                        .addLast(new IdleStateHandler(0, 4, 0, TimeUnit.SECONDS))
//                        .addLast(afterChannelHandlers.toArray());
            }
        });

        ThreadPoolExecutor runThread = ThreadFactory.createSingle("replica-pool-%d");

        // 单独线程进行连接，防止当前调用线程阻塞
        runThread.execute(() -> {
            try {
                // 发起连接请求
                channelFuture = bootstrap.connect(this.remote.getHost(), isBft ? this.remote.getBftPort() : this.remote.getMsgPort()).sync();
                boolean isStartSuccess = channelFuture.isSuccess();
                if (isStartSuccess) {
                    // 启动成功
                    // 设置ChannelFuture对象，以便于发送的连接状态处理
                    watchDogHandler.initChannelFuture(channelFuture);
                    // 等待客户端关闭连接
                    channelFuture.channel().closeFuture().sync();
                } else {
                    // 启动失败的情况下重新连接
                    runThread.execute(() -> {
                        try {
                            TimeUnit.SECONDS.sleep(5);
                            connect();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * 初始化相关配置
     *
     * @param remote
     *     远端节点
     */
    private void init(Node remote) {

        this.remote = remote;

        this.bootstrap = new Bootstrap().group(loopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    /**
     * 发送消息
     *
     * @param message
     *     消息统一接口
     */
    public void send(Message message) {

        Channel channel = watchDogHandler.channelFuture().channel();

        channel.writeAndFlush(message);

//        channel.eventLoop().execute(() -> channel.writeAndFlush(message));
    }

    @Override
    public void close() throws Exception {
        loopGroup.shutdownGracefully();
    }

    /**
     * ChannelHandler集合管理类
     */
    public static class ChannelHandlers {

        private List<ChannelHandler> channelHandlers = new ArrayList<>();

        /**
         * 添加指定的ChannelHandler
         *
         * @param channelHandler
         *     需要加入的ChannelHandler
         * @return
         */
        public ChannelHandlers addHandler(ChannelHandler channelHandler) {
            channelHandlers.add(channelHandler);
            return this;
        }

        /**
         * List集合转换为数组
         *
         * @return
         */
        public ChannelHandler[] toArray() {
            ChannelHandler[] channelHandlerArray = new ChannelHandler[channelHandlers.size()];
            return channelHandlers.toArray(channelHandlerArray);
        }
    }
}
