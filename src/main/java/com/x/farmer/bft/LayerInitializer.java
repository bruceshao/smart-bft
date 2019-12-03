package com.x.farmer.bft;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.x.farmer.bft.client.replica.ReplicaClient;
import com.x.farmer.bft.client.replica.ReplicaClientPool;
import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.data.RequestDatas;
import com.x.farmer.bft.event.CallBackListenerEvent;
import com.x.farmer.bft.event.accept.AcceptMsgListenerEventFactory;
import com.x.farmer.bft.event.accept.AcceptMsgListenerEventHandler;
import com.x.farmer.bft.event.accept.AcceptMsgListenerEventProducer;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventFactory;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventHandler;
import com.x.farmer.bft.event.leader.LeaderChangeMsgListenerEventProducer;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventFactory;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventHandler;
import com.x.farmer.bft.event.request.TimeoutRequestMsgListenerEventProducer;
import com.x.farmer.bft.event.write.WriteMsgListenerEventFactory;
import com.x.farmer.bft.event.write.WriteMsgListenerEventHandler;
import com.x.farmer.bft.event.write.WriteMsgListenerEventProducer;
import com.x.farmer.bft.execute.AcceptMessageHandler;
import com.x.farmer.bft.execute.DefaultProposeMessageHandler;
import com.x.farmer.bft.execute.WriteMessageHandler;
import com.x.farmer.bft.listener.CallBackListenerPool;
import com.x.farmer.bft.message.AcceptMessage;
import com.x.farmer.bft.message.LeaderChangeMessage;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.message.WriteMessage;
import com.x.farmer.bft.replica.Node;
import com.x.farmer.bft.server.ConsensusServer;
import com.x.farmer.bft.server.RequestServer;
import com.x.farmer.bft.server.event.propose.ProposeMessageEvent;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventFactory;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventHandler;
import com.x.farmer.bft.server.event.propose.ProposeMessageEventProducer;
import com.x.farmer.bft.server.event.request.RequestMessageEvent;
import com.x.farmer.bft.server.event.request.RequestMessageEventFactory;
import com.x.farmer.bft.server.event.request.RequestMessageEventHandler;
import com.x.farmer.bft.server.event.request.RequestMessageEventProducer;
import com.x.farmer.bft.service.ProposeMessageHandler;
import com.x.farmer.bft.timer.RequestMessageTimer;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEvent;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEventFactory;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEventHandler;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEventProducer;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class LayerInitializer implements AutoCloseable {

    private ViewController viewController;

    private CallBackListenerPool listenerPool;

    private RequestDatas requestDatas;

    private ReplicaClientPool replicaClientPool;

    private ConsensusServer consensusServer;

    private RequestServer requestServer;

    private ProposeMessageHandler proposeMessageHandler;

    private RequestMessageEventProducer requestMessageEventProducer;

    private AcceptMsgListenerEventProducer acceptMsgListenerProducer;

    private AcceptMsgListenerEventHandler acceptMsgListenerHandler;

    private WriteMsgListenerEventProducer writeMsgListenerProducer;

    private ProposeMessageEventProducer proposeMessageEventProducer;

    private TimeoutRequestMsgListenerEventProducer timeoutListenerProducer;

    private LeaderChangeMsgListenerEventProducer leaderChangeListenerProducer;

    private LeaderChangeMsgListenerEventHandler leaderChangeListenerHandler;

    private RequestMsgTimeoutEventProducer requestTimeoutEventProducer;

    private RequestMessageTimer requestMessageTimer;

    private LayerEngine layerEngine;

    public LayerInitializer(ViewController viewController) {
        this.viewController = viewController;
    }

    public void init() {
        /**
         * 需要初始化的对象包括
         * RequestDatas requestDatas,
         * ReplicaClientPool replicaClientPool,
         * ConsensusServer consensusServer,
         * TimeoutRequestMsgListenerEventProducer writeMsgListenerProducer,
         * ProposeMessageHandler proposeMessageHandler
         */
        requestDatas = new RequestDatas();
        replicaClientPool = initClientPool(viewController.getRemotes());
        listenerPool = initListenerPool();

        requestMessageEventProducer = initRequestMessageEventProducer();
        consensusServer = initConsensusServer();
        requestServer = initRequestServer();
        proposeMessageHandler = new ProposeMessageHandler(viewController, new DefaultProposeMessageHandler());

        AcceptMsgListenerDisruptor acceptMsgListenerDisruptor = initAcceptMsgListenerDisruptor();

        acceptMsgListenerProducer = acceptMsgListenerDisruptor.getEventProducer();
        acceptMsgListenerHandler = acceptMsgListenerDisruptor.getEventHandler();

        writeMsgListenerProducer = initWriteMsgListenerDisruptor();
        proposeMessageEventProducer = initProposeMessageDisruptor();

        LeaderChangeMsgListenerDisruptor leaderChangeMsgListenerDisruptor = initLeaderChangeListenerDisruptor();
        leaderChangeListenerProducer = leaderChangeMsgListenerDisruptor.getEventProducer();
        leaderChangeListenerHandler = leaderChangeMsgListenerDisruptor.getEventHandler();

        timeoutListenerProducer = initTimeoutListenerDisruptor();
        requestTimeoutEventProducer = initRequestTimeoutEventDisruptor();

        requestMessageTimer = new RequestMessageTimer(requestDatas, viewController.getBatchTimeout(),
                                    requestTimeoutEventProducer);

        layerEngine = initLayerEngine();
    }

    public void start() throws Exception {

        // 将Disruptor加入到Server
        consensusServer.init(listenerPool,
                timeoutListenerProducer, proposeMessageEventProducer, leaderChangeListenerProducer);
        acceptMsgListenerHandler.initLayerEngineAndLeaderChangeListener(layerEngine, leaderChangeListenerProducer);
        leaderChangeListenerHandler.initLayerEngine(layerEngine);
        requestDatas.initTimer(requestMessageTimer);

        // 将相关的线程启动
        requestServer.listen();
        consensusServer.listen();
        requestMessageTimer.listen();

        // 增加适当的延时，等待上述线程启动完成后再启动
        Thread.sleep(5000);
        replicaClientPool.connect();

        ExecutorService layerEngineThreadPool = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("layer-engine-pool-%d")
                        .daemon(true).build());

        layerEngineThreadPool.execute(layerEngine);
    }


    private ReplicaClientPool initClientPool(List<Node> remotes) {

        List<ReplicaClient> replicaClients = new ArrayList<>();

        for (Node remote : remotes) {
            replicaClients.add(new ReplicaClient(viewController, remote));
        }

        return new ReplicaClientPool(replicaClients);
    }

    private CallBackListenerPool initListenerPool() {
        return new CallBackListenerPool(viewController.getTotalSize());
    }

    private ConsensusServer initConsensusServer() {
        return new ConsensusServer(viewController);
    }

    private RequestServer initRequestServer() {
        return new RequestServer(viewController, requestMessageEventProducer);
    }

    private RequestMessageEventProducer initRequestMessageEventProducer() {
        // 创建一个工厂
        RequestMessageEventFactory factory = new RequestMessageEventFactory();
        // 创建Disruptor，多生产者（默认为多生产者）
        Disruptor<RequestMessageEvent> disruptor = new Disruptor<>(
                factory, RequestMessageEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE);
        RequestMessageEventHandler eventHandler = new RequestMessageEventHandler(requestDatas);

        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<RequestMessageEvent> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new RequestMessageEventProducer(ringBuffer);
    }

    private AcceptMsgListenerDisruptor initAcceptMsgListenerDisruptor() {
        // 创建一个工厂
        AcceptMsgListenerEventFactory factory = new AcceptMsgListenerEventFactory();
        // 创建Disruptor，单生产者
        Disruptor<CallBackListenerEvent<AcceptMessage>> disruptor = new Disruptor<>(
                factory, AcceptMsgListenerEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new BlockingWaitStrategy());

        AcceptMsgListenerEventHandler eventHandler = new AcceptMsgListenerEventHandler(
                viewController, listenerPool, replicaClientPool,
                consensusServer, new AcceptMessageHandler(), requestDatas);

        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<CallBackListenerEvent<AcceptMessage>> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new AcceptMsgListenerDisruptor(eventHandler, new AcceptMsgListenerEventProducer(ringBuffer));
    }

    private WriteMsgListenerEventProducer initWriteMsgListenerDisruptor() {
        // 创建一个工厂
        WriteMsgListenerEventFactory factory = new WriteMsgListenerEventFactory();

        // 创建Disruptor，单生产者
        Disruptor<CallBackListenerEvent<WriteMessage>> disruptor = new Disruptor<>(
                factory, WriteMsgListenerEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new BlockingWaitStrategy());

        WriteMsgListenerEventHandler eventHandler = new WriteMsgListenerEventHandler(
                viewController, listenerPool, replicaClientPool, consensusServer,
                new WriteMessageHandler(), acceptMsgListenerProducer);
        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<CallBackListenerEvent<WriteMessage>> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new WriteMsgListenerEventProducer(ringBuffer);
    }

    private ProposeMessageEventProducer initProposeMessageDisruptor() {
        // 创建一个工厂
        ProposeMessageEventFactory factory = new ProposeMessageEventFactory();

        // 创建Disruptor，多生产者
        Disruptor<ProposeMessageEvent> disruptor = new Disruptor<>(
                factory, ProposeMessageEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE);
        ProposeMessageEventHandler eventHandler = new ProposeMessageEventHandler(
                viewController, listenerPool, proposeMessageHandler, consensusServer,
                replicaClientPool, writeMsgListenerProducer
        );
        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<ProposeMessageEvent> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new ProposeMessageEventProducer(ringBuffer);
    }

    private LeaderChangeMsgListenerDisruptor initLeaderChangeListenerDisruptor() {
        // 创建一个工厂
        LeaderChangeMsgListenerEventFactory factory = new LeaderChangeMsgListenerEventFactory();

        // 创建Disruptor，多生产者
        Disruptor<CallBackListenerEvent<LeaderChangeMessage>> disruptor = new Disruptor<>(
                factory, TimeoutRequestMsgListenerEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE);

        LeaderChangeMsgListenerEventHandler eventHandler = new LeaderChangeMsgListenerEventHandler(
                viewController, consensusServer);
        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<CallBackListenerEvent<LeaderChangeMessage>> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new LeaderChangeMsgListenerDisruptor(eventHandler,
                new LeaderChangeMsgListenerEventProducer(ringBuffer));
    }

    private TimeoutRequestMsgListenerEventProducer initTimeoutListenerDisruptor() {
        // 创建一个工厂
        TimeoutRequestMsgListenerEventFactory factory = new TimeoutRequestMsgListenerEventFactory();

        // 创建Disruptor，多生产者
        Disruptor<CallBackListenerEvent<RequestTimeoutMessage>> disruptor = new Disruptor<>(
                factory, TimeoutRequestMsgListenerEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE);

        TimeoutRequestMsgListenerEventHandler eventHandler = new TimeoutRequestMsgListenerEventHandler(
                viewController, listenerPool, replicaClientPool, consensusServer, leaderChangeListenerProducer);
        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<CallBackListenerEvent<RequestTimeoutMessage>> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new TimeoutRequestMsgListenerEventProducer(ringBuffer);
    }


    private RequestMsgTimeoutEventProducer initRequestTimeoutEventDisruptor() {
        // 创建一个工厂
        RequestMsgTimeoutEventFactory factory = new RequestMsgTimeoutEventFactory();

        // 创建Disruptor，单生产者
        Disruptor<RequestMsgTimeoutEvent> disruptor = new Disruptor<>(
                factory, TimeoutRequestMsgListenerEventFactory.RING_BUFFER, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new BlockingWaitStrategy());

        RequestMsgTimeoutEventHandler eventHandler = new RequestMsgTimeoutEventHandler(
                listenerPool, consensusServer, replicaClientPool, timeoutListenerProducer);
        // 连接Handler（即消费者）
        disruptor.handleEventsWith(eventHandler);
        // 启动Disruptor
        disruptor.start();
        // 获取RingBuffer
        RingBuffer<RequestMsgTimeoutEvent> ringBuffer = disruptor.getRingBuffer();
        // 创建生产者
        return new RequestMsgTimeoutEventProducer(ringBuffer);
    }

    private LayerEngine initLayerEngine() {
        return new LayerEngine(requestDatas,
                viewController,
                replicaClientPool,
                consensusServer,
                proposeMessageEventProducer,
                proposeMessageHandler);
    }

    @Override
    public void close() throws Exception {
        consensusServer.close();
        requestServer.close();
        replicaClientPool.close();
    }


    private static class LeaderChangeMsgListenerDisruptor {

        private LeaderChangeMsgListenerEventHandler eventHandler;

        private LeaderChangeMsgListenerEventProducer eventProducer;

        public LeaderChangeMsgListenerDisruptor(LeaderChangeMsgListenerEventHandler eventHandler,
                                                LeaderChangeMsgListenerEventProducer eventProducer) {
            this.eventHandler = eventHandler;
            this.eventProducer = eventProducer;
        }

        public LeaderChangeMsgListenerEventHandler getEventHandler() {
            return eventHandler;
        }

        public LeaderChangeMsgListenerEventProducer getEventProducer() {
            return eventProducer;
        }
    }

    private static class AcceptMsgListenerDisruptor {

        private AcceptMsgListenerEventHandler eventHandler;

        private AcceptMsgListenerEventProducer eventProducer;

        public AcceptMsgListenerDisruptor(AcceptMsgListenerEventHandler eventHandler,
                                          AcceptMsgListenerEventProducer eventProducer) {
            this.eventHandler = eventHandler;
            this.eventProducer = eventProducer;
        }

        public AcceptMsgListenerEventHandler getEventHandler() {
            return eventHandler;
        }

        public AcceptMsgListenerEventProducer getEventProducer() {
            return eventProducer;
        }
    }
}
