package com.example.collectorexample002.request.record;

import java.time.LocalDateTime;

public record ParseData(
        Long checkpointId,
        int checkpointAddress,
        String description,
        String dataType,
        String dataUnit,
        Object parsedValue,
        LocalDateTime collectedAt
) {
}
