package com.example.collectorexample002.modbus;

import com.example.collectorexample002.modbus.record.ModbusRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ModbusRequestManager {

    public static final Map<Integer, ModbusRequest> RESPONSE_PROMISE = new ConcurrentHashMap<>();
}
