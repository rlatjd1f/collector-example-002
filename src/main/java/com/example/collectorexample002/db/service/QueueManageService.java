package com.example.collectorexample002.db.service;

import com.example.collectorexample002.netty.request.record.CheckpointQueueData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

@Component
@Slf4j
public class QueueManageService {

    private final LinkedBlockingQueue<CheckpointQueueData> redisQueue = new LinkedBlockingQueue<>(5000);
    private final LinkedBlockingQueue<CheckpointQueueData> kafkaQueue = new LinkedBlockingQueue<>(5000);

    public void pushToRedis(CheckpointQueueData checkpointQueueData) {
        if (!redisQueue.offer(checkpointQueueData)) {
            log.error("Redis 큐 버퍼 overflow Error");
        }
    }

    public void pushToKafka(CheckpointQueueData checkpointQueueData) {
        if (!kafkaQueue.offer(checkpointQueueData)) {
            log.error("Kafka 큐 버퍼 overflow Error");
        }
    }

    public CheckpointQueueData redisQueuePolling() throws InterruptedException {
        return redisQueue.take();
    }

    public CheckpointQueueData kafkaQueuePolling() throws InterruptedException {
        return kafkaQueue.take();
    }

    public int getRedisQueueSize() {
        return redisQueue.size();
    }

    public int getKafkaQueueSize() {
        return kafkaQueue.size();
    }
}
