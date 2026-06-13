package com.example.collectorexample002.modbus;

import com.example.collectorexample002.modbus.record.ModbusMbapHeader;
import io.netty.util.AttributeKey;

public class ModbusContext {

    // Netty 채널에 deviceId 를 저장하기 위한 키 정의
    public static final AttributeKey<Long> DEVICE_ID_KEY = AttributeKey.valueOf("deviceId");

    public static final AttributeKey<ModbusMbapHeader> CURRENT_HEADER_KEY = AttributeKey.valueOf("currentHeader");
}
