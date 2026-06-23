package com.example.collectorexample002.db.service;

import com.example.collectorexample002.request.record.DataLogRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Slf4j
public class DataQueueService {

    // Redis 실패시 큐의 맨 앞으로 데이터를 다시 넣기 위해 Deque 사용
    private final LinkedBlockingDeque<DataLogRequest> redisQueue = new LinkedBlockingDeque<>(10000);
    // Kafka 설정으로 재시도 옵션 처리를 넣어서 Deque 대신 Queue 사용
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

    /**
     * Redis 작업 실패시 큐의 맨앞으로 넣기위한 작업
     * @param dataLogRequest
     */
    public void pushToFrontRedis(DataLogRequest dataLogRequest) {
        try{
            redisQueue.putFirst(dataLogRequest);
            log.info("[REDIS_QUEUE] redis 큐 데이터 처리중 문제 발생으로 데이터의 putFirst 실행, queue cnt: {}", redisQueue.size());
        } catch (InterruptedException e) {
            log.error("[REDIS_QUEUE] redis 큐 putFirst 처리 도중 오류 발생", e);
            Thread.currentThread().interrupt();
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
