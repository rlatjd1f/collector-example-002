package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.protocol.Protocols;
import com.example.collectorexample002.request.record.DataLogRequest;
import com.example.collectorexample002.request.record.CheckpointData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.core.RedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class RedisInboundHandler extends ChannelInboundHandlerAdapter {

    private final static String REDIS_KEY_FORMAT = "CHECK_POINT_%s_%s";
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // msg 타입 검증
        if(!(msg instanceof DataLogRequest dataLogRequest)) {
            ctx.fireChannelRead(msg);
            return;
        }

        // 체크포인트를 주소별로 담기위한 Map
        Map<String, Object> checkpointMap = getCheckpointMap(dataLogRequest);

        // Redis 작업 비동기로 실행
        try {
            CompletableFuture.runAsync(() -> {
                try {
                    // 장비명과 프로토콜을 redis 키에 지정하여 동일 장비여도 프로토콜 별로 데이터를 처리하기 위함
                    String redisKey = String.format(REDIS_KEY_FORMAT, dataLogRequest.deviceName(), dataLogRequest.protocolName());

                    // 동일한 key에 대해 갱신, 기존 key 없으면 새로 입력
                    redisTemplate.opsForHash().putAll(redisKey, checkpointMap);
                    log.info("[RedisInboundHandler] redis 전송 성공, key: {}", redisKey);
                } catch (Exception e) {
                    log.error("[RedisInboundHandler] redis 전송 오류", e);
                }
            });
        } catch (Exception e) {
            log.error("json 변환 에러", e);
        }

        ctx.fireChannelRead(dataLogRequest);
    }

    private @NonNull Map<String, Object> getCheckpointMap(DataLogRequest dataLogRequest) {
        Map<String, Object> checkpointMap = new HashMap<>();
        List<CheckpointData> checkpointDataList = dataLogRequest.checkpointDataList();

        // MODBUS 프로토콜 Hash key: 레지스터 주소, value: 체크포인트 값
        if (dataLogRequest.protocolName().equals(Protocols.MODBUS.name())) {
            checkpointDataList.forEach(checkpointData -> {
                String fieldKey = String.valueOf(checkpointData.checkpointAddress());
                checkpointMap.put(fieldKey, objectMapper.writeValueAsString(checkpointData));
            });
        }
        return checkpointMap;
    }
}