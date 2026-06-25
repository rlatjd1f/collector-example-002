package com.example.collectorexample002.db.record;

public record DeviceInterface(
        Long deviceId,
        Long protocolId,
        Long interface_id,
        String protocolName,
        int unitId,
        String deviceName,
        String deviceHost,
        int devicePort
) {
}
