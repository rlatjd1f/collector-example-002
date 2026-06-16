package com.example.collectorexample002.netty;

import com.example.collectorexample002.db.DeviceJdbcRepository;
import com.example.collectorexample002.db.EnumJdbcRepository;
import com.example.collectorexample002.db.record.Checkpoints;
import com.example.collectorexample002.checkpoint.record.CheckpointRequest;
import com.example.collectorexample002.checkpoint.CheckpointRequestManager;
import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.db.record.EnumDetail;
import com.example.collectorexample002.netty.config.ChannelAttributes;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.Timer;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class NettyModbusClientManager {

    private final DeviceJdbcRepository deviceJdbcRepository;
    private final EnumJdbcRepository enumJdbcRepository;
    private final AtomicInteger transactionIdGenerator = new AtomicInteger(0);

    private final EventLoopGroup group;
    private final Bootstrap modbusBootstrap;
    private final Timer hashedWheelTimer;

    private static final int MAX_REQUEST_REGISTERS = 125;

    @Value("${collector.polling_cycle}")
    private int polling_cycle;

    @Value("${collector.connection_timeout}")
    private int connection_timeout;

    @Value("${collector.response_timeout}")
    private int response_timeout;

    public void connect(Device device) {
        String host = device.deviceHost();
        int port = device.devicePort();

        log.info("디바이스 연결 시도 [{}]( {}:{} )...", device.deviceName(), host, port);

        List<Checkpoints> checkpoints = deviceJdbcRepository.findCheckpointByDeviceId(device.deviceId());
        if (checkpoints.isEmpty()) {
            // 디바이스에 등록된 레지스터 정보가 없으면 폴링할 정보가 없다는뜻
            log.warn("{} 디바이스에 등록된 레지스터 정보가 없음, 종료", device.deviceName());
            return;
        }

        Map<Long, Map<Integer, String>> enumMap = enumJdbcRepository.findAllEnumDetail()
                .stream()
                .collect(Collectors.groupingBy(EnumDetail::enumId,
                        Collectors.toMap(EnumDetail::enumCode, EnumDetail::enumDetailName)));

        // 연속된 주소로 요청가능한 레지스터들을 블록단위로 생성
        Map<Integer, List<Checkpoints>> readBlocks = makeReadBlocks(checkpoints);

        modbusBootstrap.connect(host, port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                log.info("디바이스 연결 성공 -> {}", device.deviceName());
                polling(future.channel(), device, readBlocks, enumMap);
            } else {
                log.error("디바이스 연결 timeout 실패 -> {}, 10초후  재연결 시도", device.deviceName());
                hashedWheelTimer.newTimeout(timeout -> connect(device), connection_timeout, TimeUnit.SECONDS);
            }
        });
    }

    public void polling(Channel channel, Device device, Map<Integer, List<Checkpoints>> readBlocks, Map<Long, Map<Integer, String>> enumMap) {

        if(!channel.isActive()) {
            log.warn("{} 디바이스 채널 비활성화로 채널 종료, 재연결 시도", device.deviceName());
            hashedWheelTimer.newTimeout(timeout -> connect(device), connection_timeout, TimeUnit.SECONDS);
            return;
        }

        log.info("{} 디바이스 폴링 작업 시작", device.deviceName());

        channel.attr(ChannelAttributes.ENUM_MAP).set(enumMap);

        channel.eventLoop().execute(()-> {
            readBlocks.forEach((txId, checkpoints) -> {
                sendModbusRequest(channel, device.unitId(), checkpoints, txId)
                        .thenAccept(payload -> {
                            try {
                                log.info("[수신 완료] register start address = {}, count = {}", checkpoints.get(0).checkpointAddress(), checkpoints.size());
                            } finally {
                                payload.release();
                            }
                        })
                        .exceptionally(ex -> {
                            log.error("register address = {} 수집 실패, {}", checkpoints.get(0).checkpointAddress(), ex.getMessage());
                            return null;
                        });
            });
        });

        // 10초뒤에 polling 작업 재호출을 위한 타이머 등록
        hashedWheelTimer.newTimeout(timeout -> polling(channel, device, readBlocks, enumMap), polling_cycle, TimeUnit.SECONDS);
    }

    private CompletableFuture<ByteBuf> sendModbusRequest(Channel channel, int unitId, List<Checkpoints> checkpoints, Integer txId) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        // 비동기 응답처리에 사용할 future 객체와 디코딩시 필요한 register 정보 전달
        CheckpointRequestManager.REQUEST_MAP.put(txId, new CheckpointRequest(future, checkpoints));
        // 연속된 레지스터들의 시작 주소
        int checkpointAddress = checkpoints.get(0).checkpointAddress();
        // 연속된 레지스터들의 카운트 합
        int checkpointCount = checkpoints.stream().mapToInt(Checkpoints::checkpointCount).sum();

        // Modbus TCP MBAP Header + PDU
        ByteBuf requestBuffer = channel.alloc().buffer(12);

        // HEADER 7 Bytes
        requestBuffer.writeShort(txId);
        requestBuffer.writeShort(0);    // protocol id
        requestBuffer.writeShort(6);    // length
        requestBuffer.writeByte(unitId);     // unit id

        // PDU 5 Bytes
        requestBuffer.writeByte(3); // function code 0x03
        requestBuffer.writeShort(checkpointAddress);
        requestBuffer.writeShort(checkpointCount);

        channel.writeAndFlush(requestBuffer).addListener(writeFuture -> {
            if(!writeFuture.isSuccess()) {
                // 소켓 전송 실패한 경우 리턴
                CheckpointRequestManager.REQUEST_MAP.remove(txId);
                future.completeExceptionally(writeFuture.cause());
            }
        });

        // 응답 타임아웃 처리
        hashedWheelTimer.newTimeout(timeout -> {
            CheckpointRequest pendingRequest = CheckpointRequestManager.REQUEST_MAP.remove(txId);

            // 비동기 응답이 디코딩 단계에서 정상처리 된 경우 remove 되었으므로 null 반환 정상
            if (pendingRequest == null) {
                return;
            }

            // 디코딩 단계에서 remove 처리되지 않은경우 타임아웃으로 판단
            CompletableFuture<ByteBuf> removed = pendingRequest.future();
            if (removed != null && !removed.isDone()) {
                removed.completeExceptionally(new TimeoutException("응답 타임아웃"));
            }
        }, response_timeout, TimeUnit.SECONDS);

        return future;
    }

    private Map<Integer, List<Checkpoints>> makeReadBlocks(List<Checkpoints> checkpointsList) {

        Map<Integer, List<Checkpoints>> readBlocks = new HashMap<>();
        List<Checkpoints> blockRegisters = new ArrayList<>();

        int currentAddress;
        int currentCount;
        int lastAddress = 0;
        int lastCount = 0;

        int requestCount;

        for (Checkpoints checkpoints : checkpointsList) {

            currentAddress = checkpoints.checkpointAddress();
            currentCount = checkpoints.checkpointCount();
            requestCount = blockRegisters.size();

            if (requestCount == 0) {
                // 첫 레지스터는 블록의 시작
                blockRegisters.add(checkpoints);
            } else {
                // 2번째 루프부터 연속성 여부 계산
                if (currentAddress != lastAddress + lastCount || requestCount == MAX_REQUEST_REGISTERS) {
                    // address 연속성이 깨지면 블록 단위 생성
                    readBlocks.put(generateTxId(), blockRegisters);
                    blockRegisters = new ArrayList<>();
                }

                blockRegisters.add(checkpoints);
            }

            lastAddress = currentAddress;
            lastCount = currentCount;
        }

        // 나머지 checkpointList 추가
        readBlocks.put(generateTxId(), blockRegisters);
        return readBlocks;
    }

    private Integer generateTxId() {
        // transactionId 생성 0~65535 반복
        return transactionIdGenerator.incrementAndGet() & 0xFFFF;
    }

    @PreDestroy
    public void shutdown() {

        if(hashedWheelTimer != null) {
            log.info("HashedWheelTimer 종료 처리");
            hashedWheelTimer.stop();
        }

        if (group != null) {
            log.info("Netty EventLoopGroup 종료 처리");
            group.shutdownGracefully();
        }
    }
}
