package com.example.collectorexample002.modbus;

import com.example.collectorexample002.modbus.record.ModbusPendingRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ModbusRequestManager {

    public static final Map<Integer, ModbusPendingRequest> RESPONSE_PROMISE = new ConcurrentHashMap<>();
}
