package com.example.collectorexample002.db.service;

import com.example.collectorexample002.request.record.CheckpointQueueData;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaDataService {

    private final static Logger failLogger = LoggerFactory.getLogger("KAFKA_FAIL_LOGGER");
    private final static String KAFKA_TOPIC = "CHECK_POINT_TOPIC";
    private final static String FAIL_MESSAGE_PATH = "logs/fail/";
    private final static long KAFKA_SUSPEND_DURATION_MS = 10000;

    private final QueueManagerService queueService;
    private KafkaProducer<String, String> producer;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // kafka 브로커 장애 판단 플래그
    private volatile boolean isKafkaDown = false;
    private long lastFailureTime = 0;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 압축방식 설정, lz4 는 압축률은 낮지만 평균지연시간이 가장 낮고 cpu 사용률이 낮음
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // 전송 설정
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // 멱등성 체크
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);  // 5건 전송후 ack 수신
        props.put(ProducerConfig.ACKS_CONFIG, "all");               // 전송시 ack 항상 받기
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);              // 5ms 동안 메시지 모아서 발송
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);                 // 전송 타임아웃
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));  // 32KB 크기로 배치사이즈 설정

        // 재전송 설정
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);    // 재전송 횟수
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000);     // 재전송 시도 제한 시간
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);         // 재전송 시도 간격
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);             // 브로커 장애시 대기시간

        this.producer = new KafkaProducer<>(props);
        log.info("[KAFKA_INIT] Apache Kafka Producer 초기화 완료");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void eventListener() {
        Thread workerThread = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    CheckpointQueueData queueData = queueService.kafkaQueuePolling();
                    Long deviceId = queueData.deviceId();
                    Long interfaceId = queueData.interfaceId();

                    StringBuilder message = new StringBuilder();
                    String partitionKey = String.format("%s-%s", deviceId, interfaceId);

                    // 각 라인에 키 + 메시지 구조
                    queueData.checkpointDataList().forEach(checkpointData -> {
                        message.append(partitionKey).append("/")
                                  .append(checkpointData.checkpointId()).append(",")
                                  .append(checkpointData.checkpointAddress()).append(",")
                                  .append(checkpointData.parsedValue()).append(",")
                                  .append(checkpointData.collectedAt())
                                  .append(System.lineSeparator());
                    });

                    // 파티션키 생성 - 장비id + 인터페이스id 기준으로 동일파티션에 순서보장
                    sendMessage(partitionKey, message.toString());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("KAFKA 큐 서비스 적재 실패");
                }
            }
        });

        workerThread.setName("kafkaEventListener");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void sendMessage(String partitionKey, String message) {

        long currentTime = System.currentTimeMillis();

        if (isKafkaDown) {
            // kafka 다운 상태 유지할 KAFKA_SUSPEND_DURATION_MS(10s) 시간동안 fail logger 형태로 저장
            if (currentTime - lastFailureTime < KAFKA_SUSPEND_DURATION_MS) {
                failMessageWrite(message);
                return;
            }
            // KAFKA_SUSPEND_DURATION_MS 시간 이후에 장애 flag 변경하여 kafka 재전송 시도
            else {
                log.info("[KAFKA_SEND] 장애 차단시간 만료, kafka 전송 재시도");
                isKafkaDown = false;
            }
        }
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, partitionKey, message);
            producer.send(record, ((metadata, exception) -> {
                if (exception != null) {
                    log.error("[KAFKA_SEND] kafka send 실행 오류, 잔여 queue cnt: {}", queueService.getKafkaQueueSize(), exception);
                    kafkaDownTrigger();
                    failMessageWrite(message);
                } else {
                    log.info("[KAFKA_SEND] kafka send 성공, 잔여 queue cnt: {},  파티션: {}, 오프셋: {}", queueService.getKafkaQueueSize(), metadata.partition(), metadata.offset());
                }
            }));
        } catch (Exception e) {
            log.error("[KAFKA_SEND] 프로듀서 버퍼 포화상태, 파일 백업, 잔여 queue cnt: {}", queueService.getKafkaQueueSize());
            kafkaDownTrigger();
            failMessageWrite(message);
        }
    }

    /**
     * kafka 장애 발생시 재처리를 위한 Flag 값 변경
     */
    private synchronized void kafkaDownTrigger() {
        if(!isKafkaDown) {
            isKafkaDown = true;
            lastFailureTime = System.currentTimeMillis();
            log.warn("[KAFKA_SEND] kafka 장애 플래그 false => true, {} 초 동안 파일 백업 시작", KAFKA_SUSPEND_DURATION_MS);
        }
    }

    /**
     * kafka 장애 발생시 재처리를 위한 fail logger 작성
     * @param message
     */
    private void failMessageWrite(String message) {
        failLogger.info("{}", message);
        log.info("[KAFKA_SEND] 복구용 file 쓰기 작업 완료, 잔여 queue cnt: {}", queueService.getKafkaQueueSize());
    }

    /**
     * 60초 주기로 fail log 로 복구작업을 하기위한 검사
     */
    @Scheduled(fixedDelay = 60000)
    public void msgRestoreScheduler() {

        // kafka 장애상태일 경우 리턴
        if (isKafkaDown) {
            return;
        }

        File logDir = new File(FAIL_MESSAGE_PATH);
        if (!logDir.exists()) {
            return;
        }

        if (!logDir.canWrite() || !logDir.canExecute()) {
            log.warn("[KAFKA_RESTORE] 복구 로그 디렉토리[{}] 권한 부족, 실행: {}, 쓰기: {}", logDir.getName(), logDir.canExecute(), logDir.canWrite());
            return;
        }

        // kafka_fail_*.log 패턴 로그파일 검색
        File[] failLogList = logDir.listFiles((dir, name) -> name.startsWith("kafka_fail_") && name.endsWith(".log"));

        if (failLogList == null || failLogList.length == 0) {
            log.info("[KAFKA_RESTORE] 복구 대상 파일이 없음");
            return;
        }

        long currentTime = System.currentTimeMillis();
        long oneMinuteMills = 60 * 1000;

        for (File failLog : failLogList) {

            // 생성된지 1분미만의 복구 로그는 작성되고 있으므로 건너뛰기
            if (currentTime - failLog.lastModified() < oneMinuteMills) {
                continue;
            }

            log.info("[KAFKA_RESTORE] fail log 복구 작업 시작, 파일: {}", failLog.getName());
            boolean isSuccess = msgRestoreProcess(failLog);

            if (isSuccess) {
                if (failLog.delete()) {
                    log.info("[KAFKA_RESTORE] kafka 파일 복구 완료후 삭제 성공 {}", failLog.getName());
                } else {
                    log.warn("[KAFKA_RESTORE] kafka 파일 복구 완료후 삭제 실패 {}", failLog.getName());
                    failLog.renameTo(new File(String.format("%s%s.remove", FAIL_MESSAGE_PATH, failLog.getName())));
                }
            }
        }
    }

    private boolean msgRestoreProcess(File failLog) {
        try (BufferedReader reader = new BufferedReader(new FileReader(failLog))) {
            String message;
            while((message = reader.readLine()) != null) {

                if(message.trim().isEmpty()) {
                    continue;
                }

                int delimiterIdx = message.indexOf("/");
                if (delimiterIdx == - 1) {
                    continue;
                }

                if(isKafkaDown) {
                    throw new KafkaException("kafka 브로커 장애 발생");
                }

                String partitionKey = message.substring(0, delimiterIdx);
                sendMessage(partitionKey, message);
            }

            // 잔여 버퍼를 처리하기 위한 flush 호출
            producer.flush();

            return true;
        } catch (IOException e) {
            log.error("[KAFKA_RESTORE] 파일 읽기중 오류 발생", e);
            return false;
        } catch (KafkaException e) {
            log.error("[KAFKA_RESTORE] kafka 브로커 장애 발생", e);
            return false;
        } catch (Exception e) {
            log.error("[KAFKA_RESTORE] 복구 작업중 에러 발생", e);
            return false;
        }
    }

    @PreDestroy
    public void close() {
        if(producer!=null) {
            log.info("[KAFKA_INIT] Apache Kafka Producer 연결 종료 완료");
            producer.close();
        }
    }
}
