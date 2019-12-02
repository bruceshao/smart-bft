package com.x.farmer.bft.server.event.request;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.data.RequestDatas;

/**
 * 事件处理器
 *     事件的消费者
 */
public class RequestMessageEventHandler implements EventHandler<RequestMessageEvent> {

    private final RequestDatas requestDatas;

    public RequestMessageEventHandler(RequestDatas requestDatas) {
        this.requestDatas = requestDatas;
    }

    @Override
    public void onEvent(RequestMessageEvent requestMessageEvent, long sequence, boolean endOfBatch) throws Exception {

        requestDatas.addRequest(requestMessageEvent.getRequestMessage());
    }
}