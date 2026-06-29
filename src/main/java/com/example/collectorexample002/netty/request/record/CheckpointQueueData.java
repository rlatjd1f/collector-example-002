package com.example.collectorexample002.netty.request.record;

import java.util.List;

public record CheckpointQueueData(
    Long deviceId,
    Long interfaceId,
    List<CheckpointData> checkpointDataList
) {
}
