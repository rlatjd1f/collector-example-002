package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.db.record.Checkpoint;
import com.example.collectorexample002.netty.request.CheckpointRequestManager;
import com.example.collectorexample002.netty.request.record.CheckpointRequest;
import com.example.collectorexample002.netty.request.ChannelAttributes;
import com.example.collectorexample002.enums.ModbusExceptionCode;
import com.example.collectorexample002.netty.request.record.CheckpointQueueData;
import com.example.collectorexample002.netty.request.record.CheckpointData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class ModbusPacketDecoder extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof ByteBuf payload)) {
            ctx.fireChannelRead(msg);
            return;
        }

        int txId = payload.readUnsignedShort();     // transaction id
        payload.readUnsignedShort();                // protocol id
        int length = payload.readUnsignedShort();   // length
        payload.readUnsignedByte();                 // unit id
        log.debug("[MODBUS_DECODE] 패킷 분석 시작 txId: {}, unitId(1 Byte) + PDU length: {}", txId, length);

        try {

            // channelRead 진입 이후 즉시 Map 에서 삭제하면서 CheckpointRequest 반환, 응답 받았음을 알림
            CheckpointRequest pendingRequest = CheckpointRequestManager.REQUEST_MAP.remove(txId);

            if (pendingRequest == null) {
                log.warn("[MODBUS_DECODE] txId {} 에 매칭되는 요청이 없거나 타임아웃 종료", txId);
                payload.skipBytes(length - 1);
                return;
            }

            // 요청시 사용한 CompletableFuture 객체
            CompletableFuture<ByteBuf> responseFuture = pendingRequest.future();

            // function code 에러코드 검사
            int functionCode = payload.readUnsignedByte();
            if ((functionCode & 0x80) == 0x80) {
                int exceptionCode = payload.readUnsignedByte();
                log.error("[MODBUS_DECODE] Modbus function code 에러, ErrorCode = [{}] - {}",
                        exceptionCode, ModbusExceptionCode.fromCode(exceptionCode));

                if (responseFuture != null && !responseFuture.isDone()){
                    responseFuture.completeExceptionally(new RuntimeException("function code 오류: " + exceptionCode));
                }
                return;
            }

            // header length, byte count 유효성 검사
            int byteCount = payload.readUnsignedByte();
            if (length - 3 != byteCount) {
                log.error("[MODBUS_DECODE] length: {}, byteCount: {} 유효성 검증 실패", length, byteCount);

                if (responseFuture != null && !responseFuture.isDone()){
                    responseFuture.completeExceptionally(new IllegalArgumentException("헤더 length, byte count 비교 검증 오류"));
                }
                return;
            }

            Map<Long, Map<Integer, String>> enumMasterMap = ctx.channel().attr(ChannelAttributes.ENUM_MAP).get();

            // 요청시 사용했던 체크포인트 리스트 정보
            List<Checkpoint> checkpointList = pendingRequest.registers();

            // 다음 핸들러에 전달하기 위한 리스트 반환
            List<CheckpointData> checkpointDataList = parsePayloads(payload, checkpointList, byteCount, enumMasterMap);

            if (checkpointDataList == null) {
                if (responseFuture != null && !responseFuture.isDone()) {
                    responseFuture.completeExceptionally(new IllegalArgumentException("payload 파싱 실패 오류"));
                }
            }

            if (responseFuture != null && !responseFuture.isDone()) {
                Long deviceId = pendingRequest.deviceInterface().deviceId();
                Long interfaceId = pendingRequest.deviceInterface().interfaceId();
                CheckpointQueueData checkpointQueueData = new CheckpointQueueData(deviceId, interfaceId, checkpointDataList);

                // 파싱 완료후 비동기 완료처리
                responseFuture.complete(null);
                ctx.fireChannelRead(checkpointQueueData);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            payload.release();
        }
    }

    private List<CheckpointData> parsePayloads(ByteBuf payload, List<Checkpoint> checkpointList, int byteCount, Map<Long, Map<Integer, String>> enumMasterMap) {

        List<CheckpointData> checkpointDataList = new ArrayList<>();
        int endReadIndex = payload.readerIndex() + byteCount;

        // register 개별 파싱 작업
        for (Checkpoint checkpoint : checkpointList) {

            // 현재 체크포인트가 요구하는 바이트 크기 계산 (1 count = 2 Byte)
            int requireBytes = checkpoint.requestCount() * 2;

            // payload 바이트 크기 유효성 검증
            if (payload.readerIndex() + requireBytes > endReadIndex) {
                log.warn("[MODBUS_DECODE] 체크포인트 규격 요구량이 실제 수집값보다 높음, checkpoint id: {}, address: {}, count: {}",
                        checkpoint.checkpointId(),checkpoint.requestAddress(),checkpoint.requestCount());
                if (payload.readerIndex() < endReadIndex) {
                    payload.skipBytes(endReadIndex - payload.readerIndex());
                }
                return null;
            }

            Object parsedValue = null;
            Optional<String> parsedEnumValue;
            String type = checkpoint.dataType().toUpperCase();

            switch (type) {
                case "INT16U" -> parsedValue = payload.readUnsignedShort();
                case "INT16" -> parsedValue = (int) payload.readShort();
                case "INT32U" -> parsedValue = payload.readUnsignedInt();
                case "INT32" -> parsedValue = payload.readInt();
                case "INT64" -> parsedValue = payload.readLong();
                case "FLOAT32" -> parsedValue = wordSwap(payload);
                case "BITMAP" -> {
                    if (checkpoint.requestCount() == 1) {
                        parsedValue = Integer.toBinaryString(payload.readUnsignedShort());
                    } else {
                        parsedValue = Long.toBinaryString(payload.readUnsignedInt());
                    }
                }
                case "DATETIME" -> {
                    long epochSecond = payload.readUnsignedInt();
                    parsedValue = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault());
                }
                case "TIMESTAMPED_FLOAT32" -> {
                    long timestamp = payload.readUnsignedInt();
                    float floatValue = payload.readFloat();
                    LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
                    parsedValue = String.format("[Time: %s, Value: %f]", time, floatValue);
                }
                case "UTF8" -> {
                    byte[] strBytes = new byte[requireBytes];
                    payload.readBytes(strBytes);

                    // 문자열 중간에 null(0x00) 바이트 제거하여 표현
                    parsedValue = new String(strBytes, StandardCharsets.UTF_8).replaceAll("\\u0000", "");
                }

                default -> {
                    log.warn("지원하지 않는 데이터 타입");
                    payload.skipBytes(requireBytes);
                }
            }

            if (parsedValue != null) {
                // enum 타입 처리
                if (checkpoint.valueType().equals("enum")) {
                    if (parsedValue instanceof Number numValue)
                    {
                        Map<Integer, String> enumCodeMap = enumMasterMap.get(checkpoint.enumId());
                        if (enumCodeMap.isEmpty()) {
                            log.warn("[MODBUS_DECODE] checkpoint_id: {}, enumId({})에 해당되는 정보가 없음", checkpoint.checkpointId(), checkpoint.enumId());
                            continue;
                        }

                        parsedEnumValue = Optional.ofNullable(enumCodeMap.get(numValue.intValue()));

                        if (parsedEnumValue.isEmpty()) {
                            log.warn("[MODBUS_DECODE] checkpoint_id: {}, 올바르지 않은 enum 매핑 정보, enum id: {}, enum code: {}", checkpoint.checkpointId(), checkpoint.enumId(), parsedValue);
                            continue;
                        } else {
                            parsedValue = parsedEnumValue;
                        }
                    } else {
                        log.warn("[MODBUS_DECODE] checkpoint_id: {}, Number 타입이 아닌 parseValue: {}", checkpoint.checkpointId(), parsedValue);
                        continue;
                    }
                }
            } else {
                log.warn("[MODBUS_DECODE] checkpoint_id: {}, desc: {}", checkpoint.checkpointId(), checkpoint.description());
                continue;
            }

            // redis, kafka 전달용 체크포인트 리스트
            checkpointDataList.add(new CheckpointData(
            checkpoint.checkpointId(),
            checkpoint.requestAddress(),
            parsedValue,
            LocalDateTime.now()));
        }

        // 잔여 바이트 제거
        if (payload.readerIndex() < endReadIndex) {
            int skipBytes = endReadIndex - payload.readerIndex();
            payload.skipBytes(skipBytes);
        }

        return checkpointDataList;
    }

    private float wordSwap(ByteBuf payload) {
        // A B C D => C D A B
        int swapedInt = ((payload.readByte() & 0xFF) << 8)  |
                        ((payload.readByte() & 0xFF))       |
                        ((payload.readByte() & 0xFF) << 24) |
                        ((payload.readByte() & 0xFF) << 16);

        return Float.intBitsToFloat(swapedInt);
    }
}
