package com.example.collectorexample002.modbus.record;

public record ModbusMbapHeader(
        int transactionId,
        int protocolId,
        int length
) {
}
