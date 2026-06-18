package com.example.collectorexample002.db.record;

public record Checkpoints(
        Long checkpointId,
        int checkpointAddress,
        int checkpointCount,
        String dataType,
        String dataUnit,
        String calculate,
        String valueType,
        Long enumId,
        String description
) {
}
