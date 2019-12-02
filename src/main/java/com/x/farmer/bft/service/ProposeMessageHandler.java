package com.x.farmer.bft.service;

import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.execute.MessageHandler;
import com.x.farmer.bft.message.ProposeMessage;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.WriteMessage;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProposeMessageHandler {

    private ViewController viewController;

    private MessageHandler messageHandler;

    public ProposeMessageHandler(ViewController viewController, MessageHandler messageHandler) {
        this.viewController = viewController;
        this.messageHandler = messageHandler;
    }

    public WriteMessage handle(ProposeMessage proposeMessage) {

        return handle(proposeMessage, false);

    }

    public WriteMessage handle(ProposeMessage proposeMessage, boolean isCheck) {

        // 判断消息是否是Leader发送来的
        int id = proposeMessage.id();

        if (id != viewController.getLeader()) {
            // TODO 消息不是来自于Leader，放弃
            throw new IllegalStateException("Message is not from Leader !!!");
        }

        List<RequestMessage> requestMessages = proposeMessage.getRequestMessages();

        List<byte[]> commands = new ArrayList<>();

        Map<Integer, List<Long>> idAndSequences = new ConcurrentHashMap<>();

        if (!CollectionUtils.isEmpty(requestMessages)) {

            // TODO 判断消息是否合法，同时更新idAndSequences
            for (RequestMessage rm : requestMessages) {

                // TODO 暂不考虑消息是否合法
                commands.add(rm.getBody());

                idAndSequences.computeIfAbsent(rm.getId(), k -> new ArrayList<>()).add(rm.getSequence());
            }
        }

        return convertToWriteMessage(proposeMessage, messageHandler.execute(commands), idAndSequences);

    }

    private WriteMessage convertToWriteMessage(ProposeMessage proposeMessage, byte[] agreement, Map<Integer, List<Long>> idAndSequences) {

        // WriteMessage 和 AcceptMessage类似，都需要含有对应的RequestMessage的Key列表

        WriteMessage writeMessage = new WriteMessage(viewController.getLocal().getId(),
                proposeMessage.sequence(), proposeMessage.key(), agreement, idAndSequences);

        return writeMessage;
    }
}
