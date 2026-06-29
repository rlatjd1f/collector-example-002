package com.example.collectorexample002.netty.request;

import io.netty.util.AttributeKey;

import java.util.Map;

public class ChannelAttributes {

    // enum 정보 맵
    public static final AttributeKey<Map<Long, Map<Integer, String>>> ENUM_MAP = AttributeKey.valueOf("enumMap");
}
