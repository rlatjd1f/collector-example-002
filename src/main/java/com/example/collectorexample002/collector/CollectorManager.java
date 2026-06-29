package com.example.collectorexample002.collector;

import com.example.collectorexample002.db.repository.DeviceJdbcRepository;
import com.example.collectorexample002.db.record.DeviceInterface;
import com.example.collectorexample002.netty.request.ModbusClientManager;
import com.example.collectorexample002.enums.Protocols;
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
    private final ModbusClientManager modbusClientManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        // 장비목록 전체 조회후 netty 비동기 연결 호출
        List<DeviceInterface> deviceInterfaceList = deviceJdbcRepository.findAllDevice();
        deviceInterfaceList.forEach(deviceInterface -> {
            String protocolName = deviceInterface.protocolName().toUpperCase(Locale.ROOT);

            // 멀티 프로토콜 분기 처리
            if (protocolName.equals(Protocols.MODBUS.name())) {
                modbusClientManager.connect(deviceInterface);
            }
        });
    }
}
