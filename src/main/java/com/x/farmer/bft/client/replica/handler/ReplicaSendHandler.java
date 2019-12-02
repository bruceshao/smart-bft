package com.x.farmer.bft.client.replica.handler;

import com.x.farmer.bft.message.*;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class ReplicaSendHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 有数据接入
        Message message = (Message) msg;

        if (message != null && MessageType.PROPOSE == message.type()) {
            // 收到应答
            System.out.println(new String(((ResponseMessage) message).getBody()));
        }
    }
}
