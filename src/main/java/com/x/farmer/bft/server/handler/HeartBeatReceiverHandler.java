package com.x.farmer.bft.server.handler;

import com.x.farmer.bft.message.HeartBeatMessage;
import com.x.farmer.bft.message.Message;
import com.x.farmer.bft.message.MessageType;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class HeartBeatReceiverHandler extends ChannelInboundHandlerAdapter {

    private final int localId;

    public HeartBeatReceiverHandler(int localId) {
        this.localId = localId;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 判断当前收到的信息是否为心跳信息
        Message message = (Message) msg;
        if (message != null && MessageType.HEARTBEAT == message.type()) {

//            System.out.printf("Server[%s] Receive Message from [%s] \r\n", localId, message.id());

            // 收到的消息是心跳消息，此时需要回复一个心跳消息
            ctx.writeAndFlush(new HeartBeatMessage(localId));
        } else {
            // 非心跳信息的情况下交由其他Handler继续处理
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        // 出现异常直接关闭连接
        ctx.close();
    }
}
