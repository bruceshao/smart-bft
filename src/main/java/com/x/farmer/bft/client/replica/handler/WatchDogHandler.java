package com.x.farmer.bft.client.replica.handler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.x.farmer.bft.message.HeartBeatMessage;
import com.x.farmer.bft.replica.Node;
import com.x.farmer.bft.server.codec.BftMessageDecoder;
import com.x.farmer.bft.server.codec.BftMessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.Closeable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@ChannelHandler.Sharable
public class WatchDogHandler extends ChannelInboundHandlerAdapter implements Runnable, Closeable {

    private static final int M = 1024 * 1024 * 1024;

    /**
     * 当前连接活跃状态
     */
    private final AtomicBoolean currentActive = new AtomicBoolean(false);

    /**
     * 重连的控制锁
     * 防止重连过程中重复多次调用
     */
    private final Lock reconnectLock = new ReentrantLock();

    /**
     * 默认的最多重连次数
     */
    private final int maxReconnectSize = 16;

    /**
     * 默认重连的时间，下次重连时间会变长
     */
    private final int defaultReconnectSeconds = 2;

    /**
     * 标识是否正常工作中，假设不再工作则不再重连
     */
    private boolean isWorking = true;

    /**
     * 重连调度器
     */
    private ScheduledExecutorService reconnectTimer;

    /**
     * 远端的IP（域名）信息
     */
    private String hostName;

    /**
     * 远端的端口
     */
    private int port;

    private Bootstrap bootstrap;

    /**
     * 第一组Handler数组
     */
    private ChannelHandler[] frontHandlers;

    /**
     * 后一组Handler数组
     */
    private ChannelHandler[] afterHandlers;

    /**
     * 用于重连时对象重置
     */
    private ChannelFuture channelFuture;

    /**
     * 当前节点ID
     */
    private int localId;

    /**
     * 构造器
     * @param hostName
     *     远端Host
     * @param port
     *     远端端口
     * @param bootstrap
     *     Netty工作启动器
     */
    public WatchDogHandler(int localId, String hostName, int port, Bootstrap bootstrap) {
        this.localId = localId;
        this.hostName = hostName;
        this.port = port;
        this.bootstrap = bootstrap;
    }

    /**
     * 配置重连需要的Handler
     * 主要是为了对象的复用，同时有些Handler无法复用，对于每次连接请求必须要new新的对象
     * @param frontHandlers
     * @param afterHandlers
     */
    public void init(ChannelHandler[] frontHandlers, ChannelHandler[] afterHandlers) {
        this.frontHandlers = frontHandlers;
        this.afterHandlers = afterHandlers;
        initTimer();
    }

    /**
     * 初始化ChannelFuture
     *
     * @param channelFuture
     */
    public void initChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    /**
     * 返回ChannelFuture
     *
     * @return
     *     该返回对象目前未处理是否连接成功的情况
     *     调用者可直接使用，但假设发送不成功的话会存在异常抛出
     *     调用者可手动处理异常
     */
    public ChannelFuture channelFuture() {
        try {
            // 使用锁防止在重连进行过程中互相竞争
            // 一定是等待本次重连完成才返回
            reconnectLock.lock();
            return this.channelFuture;
        } finally {
            reconnectLock.unlock();
        }
    }

    /**
     * 连接成功调用
     * 该连接成功表示完全连接成功，对于TCP而言就是三次握手成功
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 调用该方法表示连接成功
        connectSuccess();

        // 连接成功发送心跳信息
        System.out.printf("WatchDog[%s] connect OK, Send HeartBeat Msg !!! \r\n", localId);
        ctx.writeAndFlush(new HeartBeatMessage(localId));

        ctx.fireChannelActive();
    }

    /**
     * 连接失败时调用
     * 此处是触发重连的入口
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        // 调用该方法时表示连接关闭了（无论是什么原因）
        // 连接关闭的情况下需要重新连接

        connectFail();

        ctx.close();

        for (int i = 0; i < maxReconnectSize; i++) {
            reconnectTimer.schedule(this, defaultReconnectSeconds << i, TimeUnit.SECONDS);
        }

        ctx.fireChannelInactive();
    }

    @Override
    public void run() {
        if (isNeedReconnect()) {
            // 重连
            try {
                reconnectLock.lock();
                if (isNeedReconnect()) {

                    bootstrap.handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(frontHandlers)
                                    .addLast(new BftMessageDecoder(M, 28, 4))
                                    .addLast(new BftMessageEncoder())
                                    .addLast(new IdleStateHandler(0, 4, 0, TimeUnit.SECONDS))
                                    .addLast(afterHandlers)
                            ;
                        }
                    });

                    channelFuture = bootstrap.connect(hostName, port);

                    // 增加监听器用于判断本次重连是否成功
                    channelFuture.addListener((ChannelFutureListener) future -> {
                        boolean isReconnectSuccess = future.isSuccess();
                        if (isReconnectSuccess) {
                            // 连接成功
                            connectSuccess();
                        } else {
                            connectFail();
                        }
                    });
                }
            } finally {
                reconnectLock.unlock();
            }
        }
    }

    private boolean isNeedReconnect() {
        return isWorking && !currentActive.get();
    }

    private void connectSuccess() {
        this.currentActive.set(true);
    }

    private void connectFail() {
        this.currentActive.set(false);
    }

    @Override
    public void close() {
        this.isWorking = false;
        this.reconnectTimer.shutdown();
    }

    /**
     * 设置调度器
     */
    private void initTimer() {
        ThreadFactory timerFactory = new ThreadFactoryBuilder()
                .setNameFormat("reconnect-pool-%d").build();

        reconnectTimer = new ScheduledThreadPoolExecutor(1, timerFactory, new ThreadPoolExecutor.AbortPolicy());
    }
}
