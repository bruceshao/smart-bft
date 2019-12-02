package com.x.farmer.bft.replica;

public class Node {

    private int id;

    private String host;

    private int bftPort;

    private int msgPort;

    public Node(int id) {
        this.id = id;
    }

    public Node(int id, String host, int bftPort, int msgPort) {
        this.id = id;
        this.host = host;
        this.bftPort = bftPort;
        this.msgPort = msgPort;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getBftPort() {
        return bftPort;
    }

    public void setBftPort(int bftPort) {
        this.bftPort = bftPort;
    }

    public int getMsgPort() {
        return msgPort;
    }

    public void setMsgPort(int msgPort) {
        this.msgPort = msgPort;
    }
}
