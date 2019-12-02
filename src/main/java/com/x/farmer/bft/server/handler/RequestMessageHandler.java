package com.x.farmer.bft.server.handler;

import com.x.farmer.bft.message.Message;
import com.x.farmer.bft.message.MessageType;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.ResponseMessage;
import com.x.farmer.bft.server.event.request.RequestMessageEventProducer;
import com.x.farmer.bft.util.MessageUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;


@ChannelHandler.Sharable
public class RequestMessageHandler extends ChannelInboundHandlerAdapter {

    private RequestMessageEventProducer producer;

    private final int localId;

    public RequestMessageHandler(int localId, RequestMessageEventProducer producer) {
        this.localId = localId;
        this.producer = producer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 有数据接入
        Message message = (Message) msg;

        if (message != null && MessageType.REQUEST == message.type()) {
            // 将消息放入队列，然后返回应答
            producer.produce((RequestMessage) message);

            ctx.writeAndFlush(new ResponseMessage(localId, message.sequence(), message.key(),
                    "ok".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
