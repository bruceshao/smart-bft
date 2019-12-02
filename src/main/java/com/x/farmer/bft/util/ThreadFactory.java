package com.x.farmer.bft.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadFactory {

    private static final int QUEUE_CAPACITY = 1024;

    public static ThreadPoolExecutor createSingle(String nameFormat) {
        java.util.concurrent.ThreadFactory timerFactory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat).build();

        ThreadPoolExecutor runThread = new ThreadPoolExecutor(1, 1,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                timerFactory,
                new ThreadPoolExecutor.AbortPolicy());

        return runThread;
    }
}
