package com.x.farmer.bft.message;

public enum MessageType {

    /**
     * 心跳
     */
    HEARTBEAT(0),

    /**
     * 客户端请求
     */
    REQUEST(1),

    /**
     * 客户端应答消息
     */
    RESPONSE(2),

    /**
     * Propose消息
     */
    PROPOSE(3),

    /**
     * Write消息
     */
    WRITE(4),

    /**
     * Accept消息
     */
    ACCEPT(5),

    /**
     * 客户端请求
     */
    REQUEST_TIMEOUT(6),

    /**
     * 领导者改变
     */
    LEADER_CHANGE(7),

    ;

    private int code;

    MessageType(int code) {
        this.code = code;
    }

    public int code() {
        return this.code;
    }
}
