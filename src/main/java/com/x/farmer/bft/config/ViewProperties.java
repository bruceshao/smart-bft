package com.x.farmer.bft.config;

import com.x.farmer.bft.replica.Node;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ViewProperties {

    public static final String BFT_CONFIG = "bft.config";

    public static final String SYSTEM_PREFIX = "system.";

    public static final String SYSTEM_BATCH_TIMEOUT = SYSTEM_PREFIX + "batchTimeout";

    public static final String SYSTEM_MAX_BATCH_SIZE = SYSTEM_PREFIX + "maxBatchSize";

    public static final String LEADER_ID = "leader.id";

    public static final String LOCAL_ID = "local.id";

    public static final String NODE_SIZE = "node.size";

    public static final String NODE_HOST_FORMAT = "node.%s.host";

    public static final String NODE_PORT_BFT_FORMAT = "node.%s.port.bft";

    public static final String NODE_PORT_MSG_FORMAT = "node.%s.port.msg";

    public static ViewController resolve(InputStream in) throws Exception {
        Properties props = new Properties();
        props.load(in);
        return resolve(props);
    }

    public static ViewController resolve(Properties props) {

        int maxBatchSize = getInt(props, SYSTEM_MAX_BATCH_SIZE);

        long batchTimeout = getLong(props, SYSTEM_BATCH_TIMEOUT);

        int leaderId = getInt(props, LEADER_ID);

        int localId = getInt(props, LOCAL_ID);

        int nodeSize = getInt(props, NODE_SIZE);

        Node localNode = new Node(localId);

        List<Node> remotes = new ArrayList<>();

        for (int i = 0; i < nodeSize; i++) {

            String host = props.getProperty(String.format(NODE_HOST_FORMAT, i));

            int bftPort = getInt(props, String.format(NODE_PORT_BFT_FORMAT, i));

            int msgPort = getInt(props, String.format(NODE_PORT_MSG_FORMAT, i));

            if (localId == i) {
                localNode.setHost(host);
                localNode.setBftPort(bftPort);
                localNode.setMsgPort(msgPort);
            } else {
                remotes.add(new Node(i, host, bftPort, msgPort));
            }
        }

        return new ViewController(leaderId, localNode, remotes, maxBatchSize, batchTimeout);
    }

    private static int getInt(Properties props, String key) {
        return Integer.parseInt(props.getProperty(key));
    }

    private static long getLong(Properties props, String key) {
        return Long.parseLong(props.getProperty(key));
    }
}
