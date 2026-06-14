package com.example.collectorexample002.db.record;

public record ModbusRegister(
        Long deviceId,
        Long registerId,
        int registerAddress,
        int registerCount,
        String dataType,
        String dataUnit,
        String permission,
        String description
) {
}
