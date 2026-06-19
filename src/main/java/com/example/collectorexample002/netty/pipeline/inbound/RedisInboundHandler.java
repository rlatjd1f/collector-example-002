package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.request.record.DataLogRequest;
import com.example.collectorexample002.request.record.ParseData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
        Map<String, Object> payloadBody = new HashMap<>();

        // 장비명과 프로토콜을 redis 키에 지정하여 동일 장비여도 프로토콜 별로 데이터를 처리하기 위함
        String redisKey = String.format(REDIS_KEY_FORMAT, dataLogRequest.deviceName(), dataLogRequest.protocolName());

        List<ParseData> parseDataList = dataLogRequest.parseDataList();
        int startAddress = parseDataList.get(0).checkpointAddress();
        int endAddress = parseDataList.get(parseDataList.size()-1).checkpointAddress();

        parseDataList.forEach(parseData -> {
            // 체크포인트 주소값이 Key, 수집값이 Value
            String fieldKey = String.valueOf(parseData.checkpointAddress());
            payloadBody.put(fieldKey, objectMapper.writeValueAsString(parseData));
        });

        // Redis 작업 비동기로 실행
        try {
            CompletableFuture.runAsync(() -> {
                try {
                    // 동일한 key에 대해 갱신, 기존 key 없으면 새로 입력
                    redisTemplate.opsForHash().putAll(redisKey, payloadBody);
                    log.info("[REDIS_CHANNEL] redis 저장 성공, key: {}, 체크포인트 주소: {} ~ {}", redisKey, startAddress, endAddress);
                } catch (Exception e) {
                    log.error("[REDIS_CHANNEL] redis 저장 오류", e);
                }
            });
        } catch (Exception e) {
            log.error("json 변환 에러", e);
        }

        ctx.fireChannelRead(dataLogRequest);
    }
}
