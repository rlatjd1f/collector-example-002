package com.example.collectorexample002.request.record;

public record ParseData(
        String deviceName,
        Long checkpointId,
        int checkpointAddress,
        String description,
        String dataType,
        String dataUnit,
        Object parsedValue
) {
}
