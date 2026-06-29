package com.example.collectorexample002.db.service;

import com.example.collectorexample002.db.record.CheckpointEnumCode;
import com.example.collectorexample002.db.repository.EnumJdbcRepository;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Getter
@Slf4j
public class EnumCodeMapService {

    private final EnumJdbcRepository enumJdbcRepository;
    private Map<Long, Map<Integer, String>> enumCodeMap;

    public void makeEnumCodeMap() {
        this.enumCodeMap = enumJdbcRepository.findAllEnumCodes()
                .stream()
                .collect(Collectors.groupingBy(CheckpointEnumCode::enumId,
                        Collectors.toMap(CheckpointEnumCode::enumCode, CheckpointEnumCode::enumValue)));
        log.info("[ENUM_CODE_INIT] checkpoint enum code map 생성 완료");
    }
}
