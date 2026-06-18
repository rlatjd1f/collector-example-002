package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.request.record.ParseData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class RedisInboundHandler extends SimpleChannelInboundHandler<List<ParseData>> {

    private final RedisTemplate<String, Object> redisTemplate;

    private final static String REDIS_KEY = "CHECK_POINT_%s";

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, List<ParseData> parseDataList) throws Exception {

        parseDataList.forEach(parseData -> {
            log.info("[{}:{}] - {} : {}", parseData.checkpointId(), parseData.checkpointAddress(), parseData.description(), parseData.parsedValue());
        });

        Map<String, Object> payloadBody = new HashMap<>();

        payloadBody.put("collectedAt", LocalDateTime.now().toString());

        parseDataList.forEach(parseData -> {
            String fieldKey = String.valueOf(parseData.checkpointAddress());
            String jsonValue = new ObjectMapper().writeValueAsString(parseData);
            payloadBody.put(fieldKey, jsonValue);
            log.info("[CACHE_DATA]\n{}", jsonValue);
        });
    }
}
