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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    // 수집 주기
    @Value("${collector.polling_cycle}")
    private int polling_cycle;

    // tcp 연결 타임아웃
    @Value("${collector.connection_timeout}")
    private int connection_timeout;

    // 데이터 요청 타임아웃
    @Value("${collector.response_timeout}")
    private int response_timeout;

    /**
     * 대상 장비에 접속 요청하는 메서드
     */
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

        // 체크포인트 enum 타입과 매핑할 enum 테이블 조회
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

    /**
     * 수집 작업 설정 메서드
     */
    public void polling(Channel channel, Device device, Map<Integer, List<Checkpoints>> readBlocks, Map<Long, Map<Integer, String>> enumMap) {

        // tcp 연결 체크
        if(!channel.isActive()) {
            log.warn("디바이스 [{}] 채널 비활성화로 채널 종료, {} 초 후에 재연결 시도", device.deviceName(), connection_timeout);
            hashedWheelTimer.newTimeout(timeout -> connect(device), connection_timeout, TimeUnit.SECONDS);
            return;
        }

        log.info("디바이스 [{}] 폴링 작업 시작", device.deviceName());
        channel.attr(ChannelAttributes.ENUM_MAP).set(enumMap);

        // 비동기 작업 응답 수신을 위한 future 리스트
        List<CompletableFuture<Void>> futureTasks = new ArrayList<>();

        // 루프 돌면서 전송 메서드의 리턴인 future 객체를 리스트에 저장
        readBlocks.forEach((txId, checkpoints) -> {
            CompletableFuture<Void> requestFuture = sendModbusRequest(channel, device.unitId(), checkpoints, txId)
                    .thenAccept(payload -> {
                        try {
                            log.info("[{}][수신 완료] 체크포인트 = {}, count = {}", txId, checkpoints.get(0).checkpointAddress(), checkpoints.size());
                        } finally {
                            payload.release();
                        }
                    })
                    .exceptionally(ex -> {
                        log.error("[{}] 체크포인트 = {} 수집 실패, {}", txId, checkpoints.get(0).checkpointAddress(), ex.getMessage());
                        return null;
                    });

            futureTasks.add(requestFuture);
        });

        // 모든 CompletableFuture 비동기 작업들이 완료되는지 감시
        CompletableFuture<Void> allTasksFuture = CompletableFuture.allOf(futureTasks.toArray(new CompletableFuture[0]));

        // 모든 비동기 작업 응답 완료시 수행
        allTasksFuture.whenComplete(((unused, throwable) -> {
            if (throwable != null) {
                log.error("전체 modbus 수집중 에러 발생");
            } else {
                log.info("전체 modbus 수집 완료");
            }

            // polling 작업 재호출을 위한 타이머 등록
            log.info("디바이스 [{}] {} 초 후에 폴링 작업 실행", device.deviceName(), polling_cycle);
            hashedWheelTimer.newTimeout(timeout -> polling(channel, device, readBlocks, enumMap), polling_cycle, TimeUnit.SECONDS);
        }));
    }

    /**
     * modbus tcp 요청 전송 메서드
     */
    private CompletableFuture<ByteBuf> sendModbusRequest(Channel channel, int unitId, List<Checkpoints> checkpoints, Integer txId) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        // 비동기 응답처리에 사용할 future 객체와 디코딩시 필요한 checkpoint 정보 전달
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
        }, response_timeout, TimeUnit.SECONDS);

        return future;
    }

    /**
     * 체크포인트(레지스터) 연속된 주소를 블록으로 만드는 메서드
     */
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
            log.info("HashedWheelTimer 종료 처리");
            hashedWheelTimer.stop();
        }

        if (group != null) {
            log.info("Netty EventLoopGroup 종료 처리");
            group.shutdownGracefully();
        }
    }
}
