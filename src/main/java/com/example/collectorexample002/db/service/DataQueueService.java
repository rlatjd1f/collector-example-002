package com.example.collectorexample002.db.service;

import com.example.collectorexample002.request.record.DataLogRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Slf4j
public class DataQueueService {

    private final LinkedBlockingQueue<DataLogRequest> redisQueue = new LinkedBlockingQueue<>(10000);
    private final LinkedBlockingQueue<DataLogRequest> kafkaQueue = new LinkedBlockingQueue<>(10000);

    public void pushToRedis(DataLogRequest dataLogRequest) {
        if (!redisQueue.offer(dataLogRequest)) {
            log.error("Redis 큐 버퍼 overflow Error");
        }
    }

    public void pushToKafka(DataLogRequest dataLogRequest) {
        if (!kafkaQueue.offer(dataLogRequest)) {
            log.error("Kafka 큐 버퍼 overflow Error");
        }
    }

    public DataLogRequest takeFromRedis() throws InterruptedException {
        return redisQueue.take();
    }

    public DataLogRequest takeFromKafka() throws InterruptedException {
        return kafkaQueue.take();
    }

    public int getRedisQueueSize() {
        return redisQueue.size();
    }

    public int getKafkaQueueSize() {
        return kafkaQueue.size();
    }
}
