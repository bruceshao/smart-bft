package com.x.farmer.bft.timer;

import com.x.farmer.bft.data.RequestDatas;
import com.x.farmer.bft.message.RequestMessage;
import com.x.farmer.bft.message.RequestTimeoutMessage;
import com.x.farmer.bft.timer.event.RequestMsgTimeoutEventProducer;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestMessageTimer implements AutoCloseable {

    private final Lock listLock = new ReentrantLock();

    private final LinkedList<RequestMessageRemark> linkedList = new LinkedList<>();

    private final RequestDatas requestDatas;

    private final long timeout;

    private RequestMsgTimeoutEventProducer requestTimeoutEventProducer;

    private RequestTimeoutRunner timeoutRunner;

    private ScheduledExecutorService timerThreadPool;

    public RequestMessageTimer(RequestDatas requestDatas, long timeout,
                               RequestMsgTimeoutEventProducer requestTimeoutEventProducer) {
        this.requestDatas = requestDatas;
        this.timeout = timeout;
        this.requestTimeoutEventProducer = requestTimeoutEventProducer;
    }

    public void listen() {

        if (timeout > 0) {

            timeoutRunner = new RequestTimeoutRunner(requestDatas, linkedList, timeout, requestTimeoutEventProducer);
            timerThreadPool = new ScheduledThreadPoolExecutor(1,
                    new BasicThreadFactory.Builder().namingPattern("timer-schedule-pool-%d")
                            .daemon(true).build());
            timerThreadPool.scheduleAtFixedRate(timeoutRunner, 0, 2, TimeUnit.SECONDS);
        }
    }

    public void addTimer(int id, long sequence) {

        if (timeoutRunner != null) {
            listLock.lock();
            try {
                linkedList.addLast(new RequestMessageRemark(id, sequence));
            } finally {
                listLock.unlock();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (timeoutRunner != null) {
            timeoutRunner.close();
        }
        if (timerThreadPool != null) {
            timerThreadPool.shutdown();
        }
    }

    private static class RequestTimeoutRunner implements Runnable, AutoCloseable {

        private boolean isWork = true;

        private final RequestDatas requestDatas;

        private final LinkedList<RequestMessageRemark> linkedList;

        private final long timeout;

        private RequestMsgTimeoutEventProducer requestTimeoutEventProducer;

        private RequestMessageRemark lastRemark;

        public RequestTimeoutRunner(RequestDatas requestDatas, LinkedList<RequestMessageRemark> linkedList, long timeout,
                                    RequestMsgTimeoutEventProducer requestTimeoutEventProducer) {
            this.requestDatas = requestDatas;
            this.linkedList = linkedList;
            this.timeout = timeout;
            this.requestTimeoutEventProducer = requestTimeoutEventProducer;
        }

        @Override
        public void run() {
            // 默认只有一个线程处理
            // 确保每次只进行一次领导者改变
            boolean currentTimerHaveHandled = false;

            while (isWork) {

                if (lastRemark == null) {
                    lastRemark = linkedList.removeFirst();
                }

                if (lastRemark == null) {
                    // 队列中没有内容
                    break;
                }

                long time = lastRemark.getTime();

                if (System.currentTimeMillis() - time < timeout) {
                    // 当前对象时间不满足，后续对象无须处理
                    break;
                }
                int id = lastRemark.getId();
                long sequence = lastRemark.getSequence();
                RequestMessage rm = requestDatas.read(id, sequence);
                if (rm == null) {
                    // 当前消息已经不存在，则继续循环
                    lastRemark = null;
                    continue;
                }
                // 否则判断当前消息状态
                if (rm.getState() < RequestMessage.STATE_PROPOSED) {
                    if (!currentTimerHaveHandled) {
                        // 当前消息未处理，将其加入到处理线程，等到其他节点同步
                        requestTimeoutEventProducer.produce(new RequestTimeoutMessage(
                                rm.getId(), rm.getSequence(), rm.getKey(), rm.getBody()));
                        currentTimerHaveHandled = true;
                    }
                } else if (rm.getState() < RequestMessage.STATE_ACCEPTED) {
                    // TODO 已经开始共识，但尚未共识完成，暂不处理
                }

                lastRemark = null;
            }
        }

        @Override
        public void close() throws Exception {
            isWork = false;
        }
    }
}
