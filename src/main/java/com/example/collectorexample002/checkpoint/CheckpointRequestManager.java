package com.example.collectorexample002.checkpoint;

import com.example.collectorexample002.checkpoint.record.CheckpointRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointRequestManager {

    public static final Map<Integer, CheckpointRequest> REQUEST_MAP = new ConcurrentHashMap<>();
}
