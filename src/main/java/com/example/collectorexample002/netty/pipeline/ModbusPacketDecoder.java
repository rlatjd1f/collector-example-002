package com.example.collectorexample002.netty.pipeline;

import com.example.collectorexample002.db.record.ModbusRegister;
import com.example.collectorexample002.modbus.ModbusRequestManager;
import com.example.collectorexample002.modbus.record.ModbusRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class ModbusPacketDecoder extends SimpleChannelInboundHandler<ByteBuf> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) throws Exception {

        int txId = payload.readUnsignedShort();
        int protocolId = payload.readUnsignedShort();
        int length = payload.readUnsignedShort();
        int unitId = payload.readUnsignedByte();

        try {
            ModbusRequest pendingRequest = ModbusRequestManager.RESPONSE_PROMISE.remove(txId);

            if (pendingRequest == null) {
                log.warn("TxId {} 에 매칭되는 요청 레지스터를 찾을수 없음, 데이터 스킵", txId);
                payload.skipBytes(length - 1);
                return;
            }

            int functionCode = payload.readUnsignedByte();
            // modbus 예외 응답 처
            if ((functionCode & 0x80) == 0x80) {
                int exceptionCode = payload.readUnsignedByte();
                log.error("Modbus 슬레이브 에러 응답 수신, ErrorCode = {}", exceptionCode);
                return;
            }

            int byteCount = payload.readUnsignedByte();

            List<ModbusRegister> registers = pendingRequest.registers();
            parsePayloads(payload, registers, byteCount, txId);

            CompletableFuture<ByteBuf> responseFuture = pendingRequest.future();

            if (responseFuture != null && !responseFuture.isDone()) {
                responseFuture.complete(payload.retain());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void parsePayloads(ByteBuf payload, List<ModbusRegister> registers, int byteCount, int txId) {

        int endReadIndex = payload.readerIndex() + byteCount;

        // register 개별 파싱 작
        for (ModbusRegister register : registers) {

            Long deviceId = register.deviceId();

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
                case "INT32" -> parsedValue = payload.readInt();
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
        }

        if (payload.readerIndex() < endReadIndex) {
            int skipBytes = endReadIndex - payload.readerIndex();
            payload.skipBytes(skipBytes);
        }
    }
}
