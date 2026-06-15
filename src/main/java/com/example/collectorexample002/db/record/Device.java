package com.example.collectorexample002.db.record;

public record Device(
        Long deviceId,
        Long protocolId,
        int unitId,
        String deviceName,
        String deviceHost,
        int devicePort
) {
}
