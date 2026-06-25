package com.example.collectorexample002.db.service;

import com.example.collectorexample002.request.record.CheckpointData;
import com.example.collectorexample002.request.record.CheckpointQueueData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class RedisDataService {

    private final QueueManagerService queueManagerService;

    private final static String REDIS_KEY_FORMAT = "CHECK_POINT_%s";
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisSerializer<String> stringRedisSerializer = StringRedisSerializer.UTF_8;

    @EventListener(ApplicationReadyEvent.class)
    public void eventListener() {
        Thread workerThread = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()) {

                CheckpointQueueData queueData;
                try {
                    queueData = queueManagerService.redisQueuePolling();

                    List<CheckpointData> dataList = queueData.checkpointDataList();
                    if (dataList == null || dataList.isEmpty()) {
                        continue;
                    }

                    redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                        dataList.forEach(data -> {
                            String key = String.format(REDIS_KEY_FORMAT,data.checkpointId());
                            String value = String.format("%s,%s", data.parsedValue(), LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

                            byte[] keyBytes = stringRedisSerializer.serialize(key);
                            byte[] valuesBytes = stringRedisSerializer.serialize(value);
                            connection.stringCommands().set(keyBytes, valuesBytes);

//                            log.info("[REDIS_PIPELINE] pipeline execute result: {}", connection.stringCommands().set(keyBytes, valuesBytes));
                        });
                        return null;
                    });

                    log.info("[REDIS_SEND] Redis send 완료, 잔여 queue cnt: {}", queueManagerService.getRedisQueueSize());

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
