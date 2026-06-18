package com.example.collectorexample002.request.record;

import java.util.List;

public record DataLogRequest(
    String deviceName,
    List<ParseData> parseDataList
) {
}
