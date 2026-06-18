package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.request.record.DataLogRequest;
import com.example.collectorexample002.request.record.ParseData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class RedisInboundHandler extends ChannelInboundHandlerAdapter {

    private final static String REDIS_KEY_FORMAT = "CHECK_POINT_%s";
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if(!(msg instanceof DataLogRequest dataLogRequest)) {
            ctx.fireChannelRead(msg);
            return;
        }

        List<ParseData> parseDataList = dataLogRequest.parseDataList();
        String deviceName = dataLogRequest.deviceName();
        String redisKey = String.format(REDIS_KEY_FORMAT, deviceName);

        Map<String, Object> payloadBody = new HashMap<>();
        parseDataList.forEach(parseData -> {
            String fieldKey = String.valueOf(parseData.checkpointAddress());
            payloadBody.put(fieldKey, objectMapper.writeValueAsString(parseData));
        });

        // Redis 저장 작업 비동기로 실행
        try {
            CompletableFuture.runAsync(() -> {
                try {
                    redisTemplate.opsForHash().putAll(redisKey, payloadBody);
                    log.info("redis 저장 성공, key: {}, value count: {}", redisKey, parseDataList.size());
                } catch (Exception e) {
                    log.error("redis 저장 오류", e);
                }
            });
        } catch (Exception e) {
            log.error("json 변환 에러", e);
        }

        ctx.fireChannelRead(dataLogRequest);
    }
}
