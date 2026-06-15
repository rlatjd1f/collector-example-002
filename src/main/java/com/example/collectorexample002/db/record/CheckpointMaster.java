package com.example.collectorexample002.db.record;

public record CheckpointMaster(
        Long deviceId,
        Long checkpointId,
        int checkpointAddress,
        int checkpointCount,
        String dataType,
        String dataUnit,
        String calculate,
        String valueType,
        Integer enumId,
        String description
) {
}
