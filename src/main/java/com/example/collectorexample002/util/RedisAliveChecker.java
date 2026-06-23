package com.example.collectorexample002.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RedisAliveChecker {

    private final RedisTemplate<String, Object> redisTemplate;

    public boolean isAlive() {
        try {
            String reply = redisTemplate.execute((RedisCallback<String>) connection -> connection.ping());
            return "PONG".equals(reply);
        } catch (Exception e) {
            log.info("[REDIS_ALIVE] Redis 연결 실패 {}", e.getMessage());
            return false;
        }
    }
}
