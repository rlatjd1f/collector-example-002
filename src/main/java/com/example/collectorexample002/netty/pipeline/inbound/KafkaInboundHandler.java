package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.request.record.DataLogRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class KafkaInboundHandler extends SimpleChannelInboundHandler<DataLogRequest> {

    private final static String KAFKA_TOPIC = "CHECK_POINT_TOPIC";
    private final static String MESSAGE_FORMAT = "%s:%s"; // deviceName:payload
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DataLogRequest dataLogRequest) throws Exception {

        try {
            String deviceName = dataLogRequest.deviceName();
            String protocolName = dataLogRequest.protocolName();

            // 파티션 키를 사용하여 메시지가 장비+프로토콜을 기준으로 일정한 파티션에 순서가 보장되게 들어가도록 한다
            String partitionKey = String.format("%s-%s", deviceName, protocolName);
            // 메시지는 장비명, 프로토콜명, 체크포인트 리스트를 포함한 json 문자열 형태
            String payload = objectMapper.writeValueAsString(dataLogRequest);

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(KAFKA_TOPIC, partitionKey, payload);
            future.whenComplete((result, throwable) -> {
                if(throwable != null) {
                    log.error("[KAFKA_SEND] kafka 전송 실패, 장비: {}, 에러: {}", deviceName, throwable.getMessage());
                } else {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("[KAFKA_SEND] kafka 전송 성공, 장비: {}, 파티션: {}, 오프셋: {}", deviceName, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA_SEND] kafka 메시지 처리중 오류 발생", e);
        }
    }
}
