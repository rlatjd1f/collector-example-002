package com.example.collectorexample002.netty.request;

import com.example.collectorexample002.db.record.Checkpoint;
import com.example.collectorexample002.db.record.DeviceInterface;
import com.example.collectorexample002.db.repository.DeviceInterfaceRepository;
import com.example.collectorexample002.db.service.EnumCodeMapService;
import com.example.collectorexample002.netty.request.record.CheckpointReadBlocks;
import com.example.collectorexample002.netty.request.record.CheckpointRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class ModbusClientManager {

    private final DeviceInterfaceRepository interfaceRepository;
    private final EnumCodeMapService enumCodeMapService;
    private final AtomicInteger transactionIdGenerator = new AtomicInteger(0);

    private final EventLoopGroup group;
    private final Bootstrap modbusBootstrap;
    private final Timer hashedWheelTimer;

    private static final int MAX_REQUEST_REGISTERS = 125;

    // 수집 주기
    @Value("${collector.polling_cycle}")
    private int pollingCycle;

    // tcp 연결 타임아웃
    @Value("${collector.connection_timeout}")
    private int connectionTimeout;

    // 데이터 요청 타임아웃
    @Value("${collector.response_timeout}")
    private int responseTimeout;

    public CheckpointReadBlocks makeReadBlocks(DeviceInterface deviceInterface) {

        Long interfaceId = deviceInterface.interfaceId();
        String protocolName = deviceInterface.protocolName();
        String deviceName = deviceInterface.deviceName();

        List<Checkpoint> checkpoints = interfaceRepository.findCheckpointByInterface(interfaceId);
        if (checkpoints.isEmpty()) {
            // 디바이스에 등록된 레지스터 정보가 없으면 폴링할 정보가 없다는뜻
            log.warn("[MODBUS_CLIENT] [{}/{}({})] 장비 / 인터페이스 등록된 체크포인트 정보가 없음, 종료", deviceName, protocolName, interfaceId);
            return null;
        }

        // 연속된 주소로 요청가능한 레지스터들을 블록단위로 생성
        Map<Integer, List<Checkpoint>> readBlocks = makeReadBlocks(checkpoints);
        return new CheckpointReadBlocks(readBlocks);
    }


    /**
     * 대상 장비에 접속 요청하는 메서드
     */
    public void pollingPrepare(DeviceInterface deviceInterface, CheckpointReadBlocks checkpointReadBlocks) {
        Long interfaceId = deviceInterface.interfaceId();
        String protocolName = deviceInterface.protocolName();
        String deviceName = deviceInterface.deviceName();
        String host = deviceInterface.deviceHost();
        int port = deviceInterface.devicePort();

        log.info("[MODBUS_CLIENT] [{}/{}({})] 연결 시도 ( {}:{} )...", deviceName, protocolName, interfaceId, host, port);

        modbusBootstrap.connect(host, port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                log.info("[MODBUS_CLIENT] [{}/{}({})] 연결 성공", deviceName, protocolName, interfaceId);
                polling(future.channel(), deviceInterface, checkpointReadBlocks);
            } else {
                log.error("[MODBUS_CLIENT] [{}/{}({})] 연결 timeout 실패, 10초후  재연결 시도", deviceName, protocolName, interfaceId);
                hashedWheelTimer.newTimeout(timeout -> pollingPrepare(deviceInterface, checkpointReadBlocks), connectionTimeout, TimeUnit.SECONDS);
            }
        });
    }

    /**
     * 수집 작업 설정 메서드
     */
    public void polling(Channel channel, DeviceInterface deviceInterface, CheckpointReadBlocks checkpointReadBlocks) {

        // tcp 연결 체크
        String deviceName = deviceInterface.deviceName();
        String protocolName = deviceInterface.protocolName();
        Long interfacedId = deviceInterface.interfaceId();

        Map<Long, Map<Integer, String>> enumMap = enumCodeMapService.getEnumCodeMap();
        Map<Integer, List<Checkpoint>> readBlocks = checkpointReadBlocks.readBlocks();

        if(!channel.isActive()) {
            log.warn("[MODBUS_CLIENT] [{}/{}({})] 채널 비활성화로 채널 종료, {} 초 후에 재연결 시도",
                    deviceName, protocolName, interfacedId, connectionTimeout);
            hashedWheelTimer.newTimeout(timeout -> pollingPrepare(deviceInterface, checkpointReadBlocks), connectionTimeout, TimeUnit.SECONDS);
            return;
        }

        log.info("[MODBUS_CLIENT] 디바이스 [{}/{}({})] 폴링 작업 시작",
                deviceName, protocolName, interfacedId);
        channel.attr(ChannelAttributes.ENUM_MAP).set(enumMap);

        // 비동기 작업 응답 수신을 위한 future 리스트
        List<CompletableFuture<Void>> futureTasks = new ArrayList<>();

        // 루프 돌면서 전송 메서드의 리턴인 future 객체를 리스트에 저장
        readBlocks.forEach((txId, checkpointList) -> {
            CompletableFuture<Void> requestFuture = sendModbusRequest(channel, deviceInterface, checkpointList, txId)
                    .thenAccept(byteBuf -> {
                        log.debug("[MODBUS_CLIENT] 체크포인트 = {} 수집 성공", checkpointList.get(0).requestAddress());
                    })
                    .exceptionally(ex -> {
                        log.error("[MODBUS_CLIENT] 체크포인트 = {} 수집 실패, {}",
                                checkpointList.get(0).requestAddress(), ex.getMessage());
                        return null;
                    });

            futureTasks.add(requestFuture);
        });

        // 모든 CompletableFuture 비동기 작업들이 완료되는지 감시
        CompletableFuture<Void> allTasksFuture = CompletableFuture.allOf(futureTasks.toArray(new CompletableFuture[0]));

        // 모든 비동기 작업 응답 완료시 수행
        allTasksFuture.whenComplete(((unused, throwable) -> {
            if (throwable != null) {
                log.error("[MODBUS_CLIENT] 전체 modbus 수집중 에러 발생");
            }

            // polling 작업 재호출을 위한 타이머 등록
            log.info("[MODBUS_CLIENT] [{}/{}({})] {} 초 후에 폴링 작업 실행",
                    deviceName, protocolName, interfacedId, pollingCycle);
            hashedWheelTimer.newTimeout(timeout ->
                    polling(channel, deviceInterface, checkpointReadBlocks), pollingCycle, TimeUnit.SECONDS);
        }));
    }

    /**
     * modbus tcp 요청 전송 메서드
     */
    private CompletableFuture<ByteBuf> sendModbusRequest(Channel channel, DeviceInterface deviceInterface, List<Checkpoint> checkpointList, Integer txId) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        // 비동기 응답처리에 사용할 future 객체, checkpoint 리스트
        CheckpointRequestManager.REQUEST_MAP.put(txId, new CheckpointRequest(future, checkpointList, deviceInterface));
        int requestStartAddress = checkpointList.get(0).requestAddress();
        int requestCountSum = checkpointList.stream().mapToInt(Checkpoint::requestCount).sum();

        ByteBuf requestBuffer = channel.alloc().buffer(12);     // Modbus TCP MBAP Header + PDU

        requestBuffer.writeShort(txId);       // transaction id
        requestBuffer.writeShort(0);    // protocol id
        requestBuffer.writeShort(6);    // length
        requestBuffer.writeByte(deviceInterface.unitId());     // unit id

        requestBuffer.writeByte(3); // function code 0x03
        requestBuffer.writeShort(requestStartAddress);
        requestBuffer.writeShort(requestCountSum);

        channel.writeAndFlush(requestBuffer).addListener(writeFuture -> {
            if(!writeFuture.isSuccess()) {
                // 소켓 전송 실패한 경우 예외처리
                CheckpointRequestManager.REQUEST_MAP.remove(txId);
                future.completeExceptionally(writeFuture.cause());
            }
        });

        // 일정시간 인바운드 핸들러로 응답 오지 않으면 타임아웃 처리
        hashedWheelTimer.newTimeout(timeout -> {
            CheckpointRequest pendingRequest = CheckpointRequestManager.REQUEST_MAP.remove(txId);

            // 인바운드 핸들러로 응답이 온경우 바로 remove 를 수행하므로 null 이면 정상으로 처리
            if (pendingRequest == null) {
                return;
            }

            // 디코딩 단계에서 remove 처리되지 않은경우 타임아웃으로 판단
            CompletableFuture<ByteBuf> removed = pendingRequest.future();
            if (removed != null && !removed.isDone()) {
                removed.completeExceptionally(new TimeoutException("응답 타임아웃"));
            }
        }, responseTimeout, TimeUnit.SECONDS);

        return future;
    }

    /**
     * 체크포인트(레지스터) 연속된 주소를 블록으로 만드는 메서드
     */
    private Map<Integer, List<Checkpoint>> makeReadBlocks(List<Checkpoint> checkpointList) {

        Map<Integer, List<Checkpoint>> readBlocks = new HashMap<>();
        List<Checkpoint> blockRegisters = new ArrayList<>();

        int currentAddress;
        int currentCount;
        int lastAddress = 0;
        int lastCount = 0;

        int requestCount;

        for (Checkpoint checkpoint : checkpointList) {

            currentAddress = checkpoint.requestAddress();
            currentCount = checkpoint.requestCount();
            requestCount = blockRegisters.size();

            if (requestCount == 0) {
                // 첫 레지스터는 블록의 시작
                blockRegisters.add(checkpoint);
            } else {
                // 2번째 루프부터 연속성 여부 계산
                if (currentAddress != lastAddress + lastCount || requestCount == MAX_REQUEST_REGISTERS) {
                    // address 연속성이 깨지면 블록 단위 생성
                    readBlocks.put(generateTxId(), blockRegisters);
                    blockRegisters = new ArrayList<>();
                }

                blockRegisters.add(checkpoint);
            }

            lastAddress = currentAddress;
            lastCount = currentCount;
        }

        // 나머지 checkpointList 추가
        readBlocks.put(generateTxId(), blockRegisters);
        return readBlocks;
    }

    /**
     * transaction Id 생성 메서드
     */
    private Integer generateTxId() {
        // transactionId 생성 0~65535 반복
        return transactionIdGenerator.incrementAndGet() & 0xFFFF;
    }

    @PreDestroy
    public void shutdown() {

        if(hashedWheelTimer != null) {
            log.info("[MODBUS_CLIENT] HashedWheelTimer 종료 처리");
            hashedWheelTimer.stop();
        }

        if (group != null) {
            log.info("[MODBUS_CLIENT] Netty EventLoopGroup 종료 처리");
            group.shutdownGracefully();
        }
    }
}
