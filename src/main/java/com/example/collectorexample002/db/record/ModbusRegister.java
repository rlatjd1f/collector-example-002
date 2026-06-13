package com.example.collectorexample002.db.record;

public record ModbusRegister(
        Long deviceId,
        Long registerId,
        Integer registerAddress,
        Integer registerCount,
        String dataType,
        String description,
        Integer pollingCycle
) {
}
