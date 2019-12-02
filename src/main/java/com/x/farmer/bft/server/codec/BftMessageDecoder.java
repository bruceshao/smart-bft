package com.x.farmer.bft.server.codec;

import com.x.farmer.bft.util.MessageUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

//@ChannelHandler.Sharable
public class BftMessageDecoder extends LengthFieldBasedFrameDecoder {

    public BftMessageDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength);
    }

    /**
     * 编码格式：
     * Header:
     * |flag|clientId|sequence|type|key |length|
     * |int |int     |long    |int |long|int   |
     * |4   |4       |8       |4   |8   |4     |
     * Body
     *
     * @param ctx
     * @param in
     *
     * @throws Exception
     */

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

        ByteBuf frame = (ByteBuf) super.decode(ctx, in);

        if (frame == null) {
            return null;
        }

        int headerData = frame.readInt();

        int id = frame.readInt();

        long sequence = frame.readLong();

        int type = frame.readInt();

        long key = frame.readLong();

        int length = frame.readInt();

        byte[] body = new byte[length];

        frame.readBytes(body);

        frame.release();

        return MessageUtils.decode(headerData, id, sequence, type, key, body);
    }

}
