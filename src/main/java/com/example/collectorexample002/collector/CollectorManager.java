package com.example.collectorexample002.collector;

import com.example.collectorexample002.db.DeviceJdbcRepository;
import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.netty.pipeline.client.NettyModbusClientManager;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Component
@RequiredArgsConstructor
public class CollectorManager implements ApplicationRunner {

    private final DeviceJdbcRepository deviceJdbcRepository;
    private final NettyModbusClientManager nettyModbusClientManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        // 장비목록 전체 조회후 netty 비동기 연결 호출
        List<Device> devices = deviceJdbcRepository.findAllDevice();
        devices.forEach(device -> {
            String protocolName = device.protocolName().toUpperCase(Locale.ROOT);

            // 멀티 프로토콜 분기 처리
            if (protocolName.equals("MODBUS")) {
                nettyModbusClientManager.connect(device);
            }
        });
    }
}
