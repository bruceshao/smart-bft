package com.x.farmer.bft.server.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

@ChannelHandler.Sharable
public class HeartBeatReceiverTrigger extends ChannelInboundHandlerAdapter {

    private int localId;

    public HeartBeatReceiverTrigger(int localId) {
        this.localId = localId;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // 服务端只会接收心跳数据后应答，而不会主动应答
        if (evt instanceof IdleStateEvent) {
            IdleState idleState = ((IdleStateEvent) evt).state();
            // 读请求超时表示很久没有收到客户端请求
            if (idleState.equals(IdleState.READER_IDLE)) {
//                System.out.printf("Server[%s] Read Message Timeout, close it !!!", localId);
                // 长时间未收到客户端请求，则关闭连接
                ctx.close();
            }
        } else {
            // 非空闲状态事件，由其他Handler处理
            super.userEventTriggered(ctx, evt);
        }
    }
}
