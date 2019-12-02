package com.x.farmer.bft.client.replica.handler;

import com.x.farmer.bft.message.HeartBeatMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

@ChannelHandler.Sharable
public class HeartBeatSenderTrigger extends ChannelInboundHandlerAdapter {

    private final int localId;

    public HeartBeatSenderTrigger(int localId) {
        this.localId = localId;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        // 心跳事件（状态空闲事件）
        if (evt instanceof IdleStateEvent) {
            IdleState idleState = ((IdleStateEvent) evt).state();
            if (idleState == IdleState.WRITER_IDLE) {
                // Sender写超时，表示很长时间没有发送消息了，需要发送消息至Receiver
//                System.out.printf("Client[%s] Write Timeout, send HeartBeat Message !!! \r\n", localId);
                ctx.writeAndFlush(new HeartBeatMessage(localId));
            }
            // TODO 还有一种情况是读写超时，该情况暂不处理
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
