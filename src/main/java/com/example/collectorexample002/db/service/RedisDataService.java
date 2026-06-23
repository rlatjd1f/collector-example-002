package com.example.collectorexample002.db.service;

import com.example.collectorexample002.request.record.CheckpointData;
import com.example.collectorexample002.request.record.DataLogRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class RedisDataService {

    private final DataQueueService queueService;

    private final static String REDIS_KEY_FORMAT = "CHECK_POINT_%s";
    private final RedisTemplate<String, Object> redisTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void eventListener() {
        Thread workerThread = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()) {

                DataLogRequest request = null;
                try {
                    request = queueService.takeFromRedis();

                    List<CheckpointData> dataList = request.checkpointDataList();
                    if (dataList == null || dataList.isEmpty()) {
                        continue;
                    }

                    redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                        dataList.forEach(data -> {
                            String key = String.format(REDIS_KEY_FORMAT,data.checkpointId());
                            String value = String.valueOf(data.parsedValue());
                            redisTemplate.opsForValue().set(key,value);
                        });
                        return null;
                    });

                    log.info("[REDIS_SEND] Redis send 완료, 잔여 queue cnt: {}", queueService.getRedisQueueSize());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RedisConnectionFailureException e) {
                    log.error("[REDIS_SEND] Redis 서버 네트워크 문제로 큐 서비스 적재 실패");
                } catch (Exception e) {
                    log.error("[REDIS_SEND] Redis 큐 서비스 적재 실패", e);
                }
            }
        });

        workerThread.setName("redisEventListener");
        workerThread.setDaemon(true);
        workerThread.start();
    }
}
