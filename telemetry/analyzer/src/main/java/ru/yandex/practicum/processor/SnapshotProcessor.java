package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.KafkaConfig;
import ru.yandex.practicum.service.SnapshotAnalyzerService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final SnapshotAnalyzerService snapshotAnalyzerService;
    private final KafkaConfig kafkaConfig;

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(snapshotConsumer::wakeup));

        try {
            snapshotConsumer.subscribe(List.of(kafkaConfig.getSnapshotConsumer().getTopic()));
            Duration timeout = Duration.ofMillis(kafkaConfig.getSnapshotConsumer().getPollTimeoutMs());

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(timeout);

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    if (record.value() != null) {
                        snapshotAnalyzerService.analyze(record.value());
                    }
                }

                if (!records.isEmpty()) {
                    snapshotConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor остановлен по сигналу завершения.");
        } catch (Exception e) {
            log.error("Ошибка в цикле обработки снапшотов", e);
        } finally {
            try {
                snapshotConsumer.commitSync();
            } finally {
                snapshotConsumer.close();
            }
        }
    }
}
