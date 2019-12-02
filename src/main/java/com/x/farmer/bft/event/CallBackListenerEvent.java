package com.x.farmer.bft.event;

import com.x.farmer.bft.listener.CallBackListener;

public class CallBackListenerEvent<T> {

    private CallBackListener<T> callBackListener;

    public CallBackListener<T> getCallBackListener() {
        return callBackListener;
    }

    public void setCallBackListener(CallBackListener<T> callBackListener) {
        this.callBackListener = callBackListener;
    }
}
