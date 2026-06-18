package com.example.collectorexample002.request.record;

import com.example.collectorexample002.db.record.Checkpoints;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public record CheckpointRequest(
        CompletableFuture<ByteBuf> future,
        List<Checkpoints> registers,
        String deviceName
) {
}
