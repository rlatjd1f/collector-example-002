package com.example.collectorexample002.netty.pipeline;

import com.example.collectorexample002.db.record.ModbusRegister;
import com.example.collectorexample002.modbus.ModbusContext;
import com.example.collectorexample002.modbus.record.ModbusMbapHeader;
import com.example.collectorexample002.modbus.record.ModbusPendingRequest;
import com.example.collectorexample002.modbus.ModbusRequestManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class ModbusBodyDecoder extends MessageToMessageDecoder<Object> {

    @Override
    protected void decode(ChannelHandlerContext ctx, Object msg, List out) throws Exception {
        if (msg instanceof ModbusMbapHeader mHeader) {
            ctx.channel().attr(ModbusContext.CURRENT_HEADER_KEY).set(mHeader);
            return;
        }

        if (msg instanceof ByteBuf payload) {

            ModbusMbapHeader activeHeader = ctx.channel().attr(ModbusContext.CURRENT_HEADER_KEY).getAndSet(null);

            // 헤더 없이 페이로드가 오면 비정상적인 경우
            if (activeHeader == null) {
                payload.release();
                return;
            }

            int txId = activeHeader.transactionId();

            try {
                Long deviceId = ctx.channel().attr(ModbusContext.DEVICE_ID_KEY).get();
                if (deviceId == null) {
                    log.error("채널에서 deviceId 를 찾을수 없음");
                    return;
                }

                int unitId = payload.readUnsignedByte();
                int functionCode = payload.readUnsignedByte();

                // 에러 응답 체크
                if ((functionCode & 0x80) == 0x80) {
                    int exceptionCode = payload.readUnsignedByte();
                    log.error("Modbus 슬레이브 에러 응답 수신, deviceId = {}, ErrorCode = {}", deviceId, exceptionCode);
                    return;
                }

                int byteCount = payload.readUnsignedByte();

                ModbusPendingRequest pendingRequest = ModbusRequestManager.RESPONSE_PROMISE.remove(txId);

                if (pendingRequest == null) {
                    log.warn("TxId {} 에 매칭되는 요청 레지스터를 찾을수 없음, 데이터 스킵", txId);
                    payload.skipBytes(byteCount);
                    return;
                }

                ModbusRegister register = pendingRequest.register();
                CompletableFuture<ByteBuf> responseFuture = pendingRequest.future();

                if (responseFuture != null && !responseFuture.isDone()) {
                    responseFuture.complete(payload.retain());
                }

                parseSinglePayload(payload, register, deviceId, byteCount, txId);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void parseSinglePayload(ByteBuf payload, ModbusRegister register, Long deviceId, int byteCount, int txId) {

        int endReadIndex = payload.readerIndex() + byteCount;
        // 현재 레지스터가 요구하는 바이트 크기 계산 (1 Register = 2 Byte)
        int requireBytes = register.registerCount() * 2;

        if (payload.readerIndex() + requireBytes > endReadIndex || payload.readableBytes() < requireBytes) {
            log.warn("TxId = {} 수신 데이터 크기가 레지스터 요구량 보다 작음", txId);
            if (payload.readerIndex() < endReadIndex) {
                payload.skipBytes(endReadIndex - payload.readerIndex());
            }
            return;
        }

        Object parsedValue = null;
        String type = register.dataType().toUpperCase();

        switch (type) {
            case "INT16U" -> parsedValue = payload.readUnsignedShort();
            case "INT16" -> parsedValue = (int) payload.readShort();
            case "INT32U" -> parsedValue = payload.readUnsignedInt();
            case "INT32" -> parsedValue = (int) payload.readInt();
            case "INT64" -> parsedValue = payload.readLong();
            case "FLOAT32" -> parsedValue = payload.readFloat();
            case "BITMAP" -> {
                if (register.registerCount() == 1) {
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
                parsedValue = new String(strBytes, StandardCharsets.UTF_8).trim();
            }

            default -> {
                log.warn("지원하지 않는 데이터 타입");
                payload.skipBytes(requireBytes);
            }
        }

        if (parsedValue != null) {
            log.info("[데이터 수집 완료] deviceId = {}, register [{}] = {}", deviceId, register.description(), parsedValue);
        }

        if (payload.readerIndex() < endReadIndex) {
            int skipBytes = endReadIndex - payload.readerIndex();
            payload.skipBytes(skipBytes);
        }
    }
}
