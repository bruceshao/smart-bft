package com.x.farmer.bft.client;

import com.x.farmer.bft.client.replica.ReplicaClient;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.replica.Node;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class BftClient {

    public static final AtomicLong sequences = new AtomicLong(1L);

    public static void main(String[] args) throws Exception {

        // -h 127.0.0.1 -p 17000 -d 0 -l 100
        if (args == null || args.length < 12) {
            throw new IllegalStateException("U should use -h 127.0.0.1 -bp 17000 -mp 18000 -d 0 -l 100 -c 100000");
        }

        ClientProp clientProp = ClientProp.resolve(args);

        Node remote = new Node(clientProp.remoteId, clientProp.host, clientProp.bftPort, clientProp.msgPort);

        ViewController viewController = new ViewController(new Node(clientProp.localId));

        // 生成一个客户端
        ReplicaClient replicaClient = new ReplicaClient(viewController, remote, false);

        replicaClient.connect();

        Thread.sleep(3000);

        for (int i = 0; i < clientProp.count; i++) {
            RequestMessage requestMessage = new RequestMessage(clientProp.localId,
                    sequences.getAndIncrement(), "www.jd.com".getBytes(StandardCharsets.UTF_8));
            replicaClient.send(requestMessage);
        }
        System.out.println("Send Request Message OK !!!");
    }


    private static class ClientProp {
        String host = "127.0.0.1";
        int bftPort = 17000;
        int msgPort = 18000;
        int remoteId = 0;
        int localId = 100;
        int count = 100000;

        public static final String ARG_HOST = "-h";

        public static final String ARG_BFT_PORT = "-bp";

        public static final String ARG_MSG_PORT = "-mp";

        public static final String ARG_ID = "-d";

        public static final String ARG_LOCAL_ID = "-l";

        public static final String ARG_COUNT = "-c";

        public static ClientProp resolve(String[] args) {

            ClientProp clientProp = new ClientProp();

            for (int i = 0; i < args.length; ) {

                String arg = args[i];
                String value = args[i + 1];

                if (arg.equals(ARG_HOST)) {
                    clientProp.host = value;
                } else if (arg.equals(ARG_BFT_PORT)) {
                    clientProp.bftPort = Integer.parseInt(value);
                } else if (arg.equals(ARG_MSG_PORT)) {
                    clientProp.msgPort = Integer.parseInt(value);
                } else if (arg.equals(ARG_ID)) {
                    clientProp.remoteId = Integer.parseInt(value);
                } else if (arg.equals(ARG_LOCAL_ID)) {
                    clientProp.localId = Integer.parseInt(value);
                } else if (arg.equals(ARG_COUNT)) {
                    clientProp.count = Integer.parseInt(value);
                }

                i += 2;
            }

            return clientProp;

        }
    }
}
