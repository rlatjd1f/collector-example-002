package com.example.collectorexample002.modbus.record;

import com.example.collectorexample002.db.record.ModbusRegister;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public record ModbusPendingRequest(
        CompletableFuture<ByteBuf> future,
        ModbusRegister register
) {
}
