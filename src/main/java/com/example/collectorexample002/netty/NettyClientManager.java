package com.example.collectorexample002.netty;

import com.example.collectorexample002.db.DeviceJdbcRepository;
import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.db.record.ModbusRegister;
import com.example.collectorexample002.modbus.ModbusContext;
import com.example.collectorexample002.modbus.record.ModbusPendingRequest;
import com.example.collectorexample002.modbus.ModbusRequestManager;
import com.example.collectorexample002.netty.pipeline.ModbusBodyDecoder;
import com.example.collectorexample002.netty.pipeline.ModbusHeaderDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class NettyClientManager {

    private final DeviceJdbcRepository deviceJdbcRepository;
    private final AtomicInteger transactionIdGenerator = new AtomicInteger(0);

    private EventLoopGroup group;
    private Bootstrap bootstrap;

    public void init() {
        log.info("netty initialize start...");
        this.group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        this.bootstrap = new Bootstrap();

        this.bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("ModbusHeaderDecoder", new ModbusHeaderDecoder());
                        ch.pipeline().addLast("ModbusBodyDecoder", new ModbusBodyDecoder());
                    }
                });
    }

    public void connect(Device device) {
        String host = device.deviceHost();
        int port = device.devicePort();

        log.info("디바이스 연결 시도 [{}]( {}:{} )...", device.deviceName(), host, port);

        bootstrap.connect(host, port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                log.info("디바이스 연결 성공 -> {}", device.deviceName());

                // 파이프라인 단계에서 device id 를 구분해서 처리하기 위해 채널에 저장
                future.channel().attr(ModbusContext.DEVICE_ID_KEY).set(device.deviceId());

                startPolling(future.channel(), device);
            } else {
                log.error("디바이스 연결 실패 -> {}, 10초후  재연결 시도", device.deviceName());

                future.channel().eventLoop().schedule(() -> connect(device), 10, TimeUnit.SECONDS);
            }
        });
    }

    public void startPolling(Channel channel, Device device) {
        List<ModbusRegister> registers = deviceJdbcRepository.findRegisterByDeviceId(device.deviceId());

        if (registers.isEmpty()) {
            log.warn("{} 디바이스에 등록된 레지스터 정보가 없음, 폴링 실패", device.deviceName());
            return;
        }

        log.info("{} 디바이스 폴링 작업 시작", device.deviceName());

        channel.eventLoop().execute(()-> {
            for (ModbusRegister register : registers) {
                sendModbusRequest(channel, register)
                        .thenAccept(payload -> {
                            try {
                                log.info("[수신 완료] register address = {}, Byte size = {}", register.registerAddress(), payload.readableBytes());
                            } finally {
                                payload.release();
                            }
                })
                        .exceptionally(ex -> {
                            log.error("register address = {} 수집 실패, {}", register.registerAddress(), ex.getMessage());
                            return null;
                        });
            }
        });

    }

    private CompletableFuture<ByteBuf> sendModbusRequest(Channel channel, ModbusRegister register) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        // 트랜잭션 id 생성 0~65535 반복
        int txId = transactionIdGenerator.incrementAndGet() & 0xFFFF;

        // 비동기 응답처리에 사용할 future 객체와 디코딩시 필요한 register 정보 전달
        ModbusRequestManager.RESPONSE_PROMISE.put(txId, new ModbusPendingRequest(future, register));

        // Modbus TCP MBAP Header + PDU
        ByteBuf requestBuffer = channel.alloc().buffer(12);

        // HEADER 7 Bytes
        requestBuffer.writeShort(txId);
        requestBuffer.writeShort(0);
        requestBuffer.writeShort(6);
        requestBuffer.writeByte(1);

        // PDU 5 Bytes
        requestBuffer.writeByte(3);
        requestBuffer.writeShort(register.registerAddress());
        requestBuffer.writeShort(register.registerCount());

        channel.writeAndFlush(requestBuffer).addListener(writeFuture -> {
            if(!writeFuture.isSuccess()) {
                // 소켓 전송 실패한 경우 리턴
                ModbusRequestManager.RESPONSE_PROMISE.remove(txId);
                future.completeExceptionally(writeFuture.cause());
            }
        });

        channel.eventLoop().schedule(() -> {
            ModbusPendingRequest pendingRequest = ModbusRequestManager.RESPONSE_PROMISE.remove(txId);

            // 비동기 응답이 디코딩 단계에서 정상처리 된 경우 remove 되었으므로 null 반환 정상
            if (pendingRequest == null) {
                return;
            }

            // 디코딩 단계에서 remove 처리되지 않은경우 타임아웃으로 판단
            CompletableFuture<ByteBuf> removed = pendingRequest.future();
            if (removed != null && !removed.isDone()) {
                removed.completeExceptionally(new TimeoutException("Modbus 응답 타임아웃"));
            }
        }, 3, TimeUnit.SECONDS);

        return future;
    }

    @PreDestroy
    public void shutdown() {
        if (group != null) {
            log.info("Netty EventLoopGroup 종료 처리");
            group.shutdownGracefully();
        }
    }
}
