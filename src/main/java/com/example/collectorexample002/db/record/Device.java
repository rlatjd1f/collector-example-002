package com.example.collectorexample002.db.record;

public record Device(
        Long deviceId,
        String protocolName,
        int unitId,
        String deviceName,
        String deviceHost,
        int devicePort
) {
}
