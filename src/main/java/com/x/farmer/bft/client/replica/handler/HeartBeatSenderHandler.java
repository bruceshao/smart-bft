package com.x.farmer.bft.client.replica.handler;

import com.x.farmer.bft.message.Message;
import com.x.farmer.bft.message.MessageType;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class HeartBeatSenderHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        Message message = (Message) msg;

        if (message != null && MessageType.HEARTBEAT == message.type()) {
            // 是心跳，只打印消息即可
//            System.out.printf("Receive HeartBeat id = {%s} \r\n", message.id());
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // 出现异常直接关闭连接
        ctx.close();
    }
}
