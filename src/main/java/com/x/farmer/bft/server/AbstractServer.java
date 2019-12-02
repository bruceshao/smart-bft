package com.x.farmer.bft.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ThreadPoolExecutor;

public abstract class AbstractServer implements AutoCloseable {

    protected static final int M = 1024 * 1024 * 1024;

    /**
     * 监听端口
     */
    protected int port;

    /**
     * 当前节点ID
     */
    protected int localId;

    /**
     * Netty中的BOSS线程
     */
    protected final EventLoopGroup bossGroup = new NioEventLoopGroup();

    /**
     * Netty中的Worker线程
     */
    protected final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public void listen() {

        ServerBootstrap bootstrap = new ServerBootstrap();

        initBootstrap(bootstrap);

        // 由单独的线程启动，防止外部调用线程阻塞
        ThreadPoolExecutor runThread = com.x.farmer.bft.util.ThreadFactory.createSingle(nameFormat());
        runThread.execute(() -> {
            try {
                ChannelFuture f = bootstrap.bind().sync();
                boolean isStartSuccess = f.isSuccess();
                if (isStartSuccess) {
                    // 启动成功
                    f.channel().closeFuture().sync();
                } else {
                    // 启动失败
                    throw new IllegalStateException("Receiver start fail :" + f.cause().getMessage() + " !!!");
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                close();
            }
        });
    }

    public int getPort() {
        return port;
    }

    public int getLocalId() {
        return localId;
    }

    protected abstract String nameFormat();

    protected abstract void initBootstrap(ServerBootstrap bootstrap);

    @Override
    public void close() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
