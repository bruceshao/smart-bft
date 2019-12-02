package com.x.farmer.bft.util;

import java.nio.ByteBuffer;

public class ByteUtils {

//    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    public static byte[] intToBytes(int i) {
        byte[] result = new byte[Integer.BYTES];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8)  & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    public static int bytesToInt(byte[] bytes){
        int num = bytes[3] & 0xFF;
        num |= ((bytes[2] << 8) & 0xFF00);
        num |= ((bytes[1] << 16)& 0xFF0000);
        num |= ((bytes[0] << 24)& 0xFF0000);
        return num;
    }

    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[Long.BYTES];
        bytes[0] = (byte) (value >> 56); //右移之后,00 00 00 00 00 00 00 08,强转之后08
        bytes[1] = (byte) (value >> 48); //右移之后,00 00 00 00 00 00 08 0A,强转之后0A
        bytes[2] = (byte) (value >> 40); //右移之后,00 00 00 00 00 08 0A 01,强转之后01
        bytes[3] = (byte) (value >> 32);
        bytes[4] = (byte) (value >> 24);
        bytes[5] = (byte) (value >> 16);
        bytes[6] = (byte) (value >> 8);
        bytes[7] = (byte) value;
        return bytes;
    }

    public static long bytesToLong(byte[] bytes) {
        //假设字节数组为08 0A 01 03 05 07 02 0B
        long b0 = bytes[0] & 0xff;//00 00 00 00 00 00 00 08
        long h0 = b0 << 56;//08 00 00 00 00 00 00 00
        long b1 = bytes[1] & 0xff;//00 00 00 00 00 00 00 0A
        long h1 = b1 << 48;//00 0A 00 00 00 00 00 00
        long b2 = bytes[2] & 0xff;
        long b3 = bytes[3] & 0xff;
        long b4 = bytes[4] & 0xff;
        long b5 = bytes[5] & 0xff;
        long b6 = bytes[6] & 0xff;
        long b7 = bytes[7] & 0xff;
        long h2 = b2 << 40;
        long h3 = b3 << 32;
        long h4 = b4 << 24;
        long h5 = b5 << 16;
        long h6 = b6 << 8;
        long h7 = b7;
        return h0 | h1 | h2 | h3 | h4 | h5 | h6 | h7;
    }

    public static byte[] merge(byte[] srcOne, byte[] srcTwo) {

        if (srcOne == null || srcOne.length == 0) {
            return srcTwo;
        }

        if (srcTwo == null || srcTwo.length == 0) {
            return srcOne;
        }

        byte[] dst = new byte[srcOne.length + srcTwo.length];

        System.arraycopy(srcOne, 0, dst, 0, srcOne.length);

        System.arraycopy(srcTwo, 0, dst, srcOne.length, srcTwo.length);

        return dst;
    }

    public static byte[] mergeAndAddLength(byte[] srcOne, byte[] srcTwo) {

        byte[] dst = new byte[Integer.BYTES + srcOne.length + srcTwo.length];

        // 复制Length（该Length指的是第一个数组）
        System.arraycopy(intToBytes(srcOne.length), 0, dst, 0, Integer.BYTES);

        // 复制第一个数组
        System.arraycopy(srcOne, 0, dst, Integer.BYTES, srcOne.length);

        // 复制第二个数组
        System.arraycopy(srcTwo, 0, dst, srcOne.length + Integer.BYTES, srcTwo.length);

        return dst;
    }
}
