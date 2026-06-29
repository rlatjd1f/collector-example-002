package com.example.collectorexample002.protocol.modbus;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum ModbusExceptionCode {
    ILLEGAL_FUNCTION(0x01, "지원하지 않는 펑션 코드"),
    ILLEGAL_DATA_ADDRESS(0x02, "존재하지 않거나 접근 불가능한 레지스터 주소"),
    ILLEGAL_DATA_VALUE(0x03, "유효하지 않은 데이터 값 또는 요청 개수 범위 초과"),
    SLAVE_DEVICE_FAILURE(0x04, "슬레이브 장비 내부 고장 및 응답 불능"),
    ACKNOWLEDGE(0x05, "장비가 명령을 접수했으나 처리 시간이 걸림"),
    SLAVE_DEVICE_BUSY(0x06, "장비가 다른 작업을 처리 중이라 바쁨"),
    MEMORY_PARITY_ERROR(0x08, "장비 내부 메모리 패리티 에러 발생"),
    GATEWAY_PATH_UNAVAILABLE(0x0A, "게이트웨이 경로 설정 오류"),
    GATEWAY_TARGET_FAILED(0x0B, "게이트웨이 대상 장비 응답 실패"),
    UNKNOWN(0x00, "정의되지 않은 알 수 없는 모드버스 에러");

    private final int code;
    private final String message;

    ModbusExceptionCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static ModbusExceptionCode fromCode(int code) {
        return Arrays.stream(values())
                .filter(e->e.code == code)
                .findFirst()
                .orElse(UNKNOWN);
    }
}
