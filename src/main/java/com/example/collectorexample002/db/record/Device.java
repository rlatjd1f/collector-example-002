package com.example.collectorexample002.db.record;

public record Device(
        Long deviceId,
        Long protocolId,
        String deviceName,
        String deviceHost,
        int devicePort
) {
}
