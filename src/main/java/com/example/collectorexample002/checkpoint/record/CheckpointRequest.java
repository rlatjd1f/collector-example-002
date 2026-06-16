package com.example.collectorexample002.checkpoint.record;

import com.example.collectorexample002.db.record.Checkpoints;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public record CheckpointRequest(
        CompletableFuture<ByteBuf> future,
        List<Checkpoints> registers
) {
}
