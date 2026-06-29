package com.example.collectorexample002.request.record;

import java.time.LocalDateTime;

/**
 * 패킷 디코딩후 Redis, Kafka 에 저장할 데이터
 * @param checkpointId      rdb 에서 체크포인트 마스터테이블의 부가 컬럼정보를 읽어오기 위한 필드
 * @param checkpointAddress redis Hash 타입 저장시 key 역할
 * @param parsedValue       체크포인트 측정 값
 * @param collectedAt       체크포인트 수집 시간
 */
public record CheckpointData(
        Long checkpointId,
        int checkpointAddress,
        Object parsedValue,
        LocalDateTime collectedAt
) {
}
