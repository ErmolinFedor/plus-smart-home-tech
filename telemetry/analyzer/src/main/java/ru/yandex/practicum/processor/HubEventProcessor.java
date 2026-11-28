package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.KafkaConfig;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final KafkaConsumer<String, HubEventAvro> hubConsumer;
    private final HubEventService hubEventService;
    private final KafkaConfig kafkaConfig;

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));

        try {
            hubConsumer.subscribe(List.of(kafkaConfig.getHubConsumer().getTopic()));
            Duration timeout = Duration.ofMillis(kafkaConfig.getHubConsumer().getPollTimeoutMs());

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(timeout);

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    if (record.value() != null) {
                        hubEventService.handle(record.value());
                    }
                }

                if (!records.isEmpty()) {
                    hubConsumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor остановлен по сигналу завершения.");
        } catch (Exception e) {
            log.error("Ошибка в цикле обработки событий хабов", e);
        } finally {
            try {
                hubConsumer.commitSync();
            } finally {
                hubConsumer.close();
            }
        }
    }
}
