package com.x.farmer.bft.execute;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AcceptMessageHandler implements MessageHandler {

    private static final int CALC_SIZE = 10000;

    private int batchSize = 1;

    private long currentTime = 0L;

    private AtomicLong counter = new AtomicLong();

    @Override
    public byte[] execute(List<byte[]> messages) {

        for (byte[] msg : messages) {

            long count = counter.incrementAndGet();

            if (count % CALC_SIZE == 0) {

                long timeRange = System.currentTimeMillis() - currentTime;

                if (timeRange == 0) {
                    timeRange = 1;
                }

                long tps = CALC_SIZE * 1000 / timeRange;

                System.out.printf("Accept Handle Message [%s] -> [%s] -> [%s] ! \r\n", Thread.currentThread().getId(), batchSize++, tps);

                currentTime = System.currentTimeMillis();
            }

        }
        return new byte[0];
    }
}
