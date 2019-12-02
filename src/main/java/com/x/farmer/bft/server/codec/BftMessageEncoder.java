package com.x.farmer.bft.server.codec;

import com.x.farmer.bft.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;


//@ChannelHandler.Sharable
public class BftMessageEncoder extends MessageToMessageEncoder<Message> {

    /**
     * 编码格式：
     * Header:
     * |flag|clientId|sequence|type|key |length|
     * |int |int     |long    |int |long|int   |
     * Body
     *
     * @param ctx
     * @param msg
     * @param out
     * @throws Exception
     */




    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {

        if (msg == null) {
            throw new IllegalStateException("The Encode Message is NULL !!!");
        }

        ByteBuf buf = Unpooled.buffer();

        // 写Flag
        buf.writeInt(Message.HEADER_DATA);
        // 写ClientID
        buf.writeInt(msg.id());
        // 写sequence
        buf.writeLong(msg.sequence());
        // 写类型
        buf.writeInt(msg.type().code());
        // 写入key
        buf.writeLong(msg.key());

        byte[] body = msg.toBytes();

        // 写长度
        buf.writeInt(body.length);

        // 写body
        buf.writeBytes(body);

        ctx.writeAndFlush(buf);
    }
}
