package com.example.collectorexample002.netty.request.record;

import com.example.collectorexample002.db.record.Checkpoint;

import java.util.List;
import java.util.Map;

public record CheckpointReadBlocks(
        Map<Integer, List<Checkpoint>> readBlocks
) {
}
