package com.example.collectorexample002.db.record;

public record Checkpoint(
        Long id,
        int requestAddress,
        int requestCount,
        String dataType,
        String dataUnit,
        String expression,
        String valueType,
        Long enumId,
        String description
) {
}
