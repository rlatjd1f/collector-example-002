package com.example.collectorexample002.collector;

import com.example.collectorexample002.db.DeviceJdbcRepository;
import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.netty.NettyClientManager;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CollectorManager implements ApplicationRunner {

    private final DeviceJdbcRepository deviceJdbcRepository;
    private final NettyClientManager nettyClientManager;

    public CollectorManager(DeviceJdbcRepository deviceJdbcRepository, NettyClientManager nettyClientManager) {
        this.deviceJdbcRepository = deviceJdbcRepository;
        this.nettyClientManager =  nettyClientManager;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        // 장비목록 전체 조회후 netty 비동기 연결 호출
        List<Device> devices = deviceJdbcRepository.findAllDevice();
        devices.forEach(nettyClientManager::connect);
    }
}
