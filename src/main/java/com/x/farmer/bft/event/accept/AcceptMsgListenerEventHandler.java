package com.x.farmer.bft.event.accept;


import com.lmax.disruptor.EventHandler;
import com.x.farmer.bft.LayerEngine;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.data.RequestDatas;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.exception.BftServiceException;
import com.x.farmer.bft.execute.MessageHandler;
import com.x.farmer.bft.listener.CallBackListener;
import com.x.farmer.bft.message.AcceptMessage;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.server.ConsensusServer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件处理器
 *     事件的消费者
 */
public class AcceptMsgListenerEventHandler implements EventHandler<CallBackListenerEvent<AcceptMessage>> {

    private ViewController viewController;

    private ConsensusServer consensusServer;

    private MessageHandler messageHandler;

    private RequestDatas requestDatas;

    private LayerEngine layerEngine;

    public AcceptMsgListenerEventHandler(ViewController viewController,
                                         ConsensusServer consensusServer,
                                         MessageHandler messageHandler,
                                         RequestDatas requestDatas) {
        this.viewController = viewController;
        this.consensusServer = consensusServer;
        this.messageHandler = messageHandler;
        this.requestDatas = requestDatas;
    }

    public void initLayerEngine(LayerEngine layerEngine) {
        this.layerEngine = layerEngine;
    }

    @Override
    public void onEvent(CallBackListenerEvent<AcceptMessage> callBackListenerEvent, long sequence, boolean endOfBatch) throws Exception {
        handleListener(callBackListenerEvent.getCallBackListener());
    }

    private void handleListener(CallBackListener<AcceptMessage> listener) {


        try {
            List<AcceptMessage> acceptMessages = listener.waitResponses(viewController.getBatchTimeout(), TimeUnit.MILLISECONDS);

            // 判断是否满足条件
            if (isEqual(acceptMessages)) {
                execute(listener.getRequestMessages(), acceptMessages.get(0));
            }
        } catch (BftServiceException e) {
            return;
        } catch (Exception e) {
            // 打印即可
            e.printStackTrace();
        } finally {
            // 不管是否满足都需要将该Listener移除
            consensusServer.removeAcceptMsgListener(listener);
            if (viewController.isLeader()) {
                layerEngine.canPropose();
            }
        }
    }

    private boolean isEqual(List<AcceptMessage> acceptMessages) {

        // 首先判断数量是否满足
        if (viewController.isMeetRule(acceptMessages.size())) {
            // 判断AcceptMessage中的内容是否一致
            byte[] agreement = acceptMessages.get(0).getAgreement();

            for (int i = 1; i < acceptMessages.size(); i++) {

                if (!Arrays.equals(agreement, acceptMessages.get(i).getAgreement())) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    private void execute(List<RequestMessage> requestMessages, AcceptMessage acceptMessage) {

        List<byte[]> commands = new ArrayList<>(requestMessages.size());

        for (RequestMessage rm : requestMessages) {

            commands.add(rm.getBody());
        }

        messageHandler.execute(commands);

        // 然后将信息移除
        requestDatas.removeRequestMessage(acceptMessage.getIdAndSequences());
    }
}