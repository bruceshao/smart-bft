package test.com.x.farmer;

import com.x.farmer.bft.LayerInitializer;
import com.x.farmer.bft.client.replica.ReplicaClient;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.config.ViewProperties;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.replica.Node;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class BftIntegrateTest {

    private static final int threadSize = 8;

    private static final AtomicLong sequences = new AtomicLong(1L);

    private static final int TOTAL_TXS = 1024 * 8;

    public static final int NODE_SIZE = 4;

    private ExecutorService threadPool = Executors.newFixedThreadPool(NODE_SIZE);

    @Test
    public void test() throws Exception {

        for (int i = 0; i < NODE_SIZE; i++) {

            try {
                InputStream inputStream = configInputStream(i);

                ViewController viewController = ViewProperties.resolve(inputStream);

                LayerInitializer layerInitializer = new LayerInitializer(viewController);

                threadPool.execute(() -> {

                    try {
                        layerInitializer.init();
                        layerInitializer.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 休眠5s，开始发送交易请求
        Thread.sleep(10000);

        clientTxs();

        Thread.sleep(Integer.MAX_VALUE);
    }

    private InputStream configInputStream(int i) {
        return BftIntegrateTest.class.getResourceAsStream("/" + String.format("bft-%s.config", i));
    }

    private void clientTxs() {

        ExecutorService threadPools = Executors.newFixedThreadPool(threadSize);

        for (int index = 0; index < threadSize; index++) {

            threadPools.execute(() -> {

                try {
                    Node remote = new Node(0, "127.0.0.1", 17000, 18000);

                    ViewController viewController = new ViewController(new Node(1024));
                    // 生成一个客户端
                    ReplicaClient replicaClient = new ReplicaClient(viewController, remote, false);

                    replicaClient.connect();

                    Thread.sleep(3000);

                    for (int i = 0; i < TOTAL_TXS; i++) {
                        RequestMessage requestMessage = new RequestMessage(1024,
                                sequences.getAndIncrement(), "www.jd.com".getBytes(StandardCharsets.UTF_8));
                        replicaClient.send(requestMessage);
                    Thread.sleep(10);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
