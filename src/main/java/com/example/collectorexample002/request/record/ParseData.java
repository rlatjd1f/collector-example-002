package com.example.collectorexample002.request.record;

import java.time.LocalDateTime;

public record ParseData(
        Long checkpointId,
        int checkpointAddress,
        Object parsedValue,
        LocalDateTime collectedAt
) {
}
