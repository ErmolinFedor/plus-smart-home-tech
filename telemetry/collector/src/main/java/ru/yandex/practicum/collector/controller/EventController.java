package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.service.EventMapperService;

@Slf4j
@Controller
@RequestMapping("/events")
public class EventController {

    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final EventMapperService mapperService;
    private final String sensorsTopic;
    private final String hubsTopic;

    public EventController(KafkaProducer<String, byte[]> kafkaProducer,
                           EventMapperService mapperService,
                           @Value("${telemetry.topics.sensors}") String sensorsTopic,
                           @Value("${telemetry.topics.hubs}") String hubsTopic) {
        this.kafkaProducer = kafkaProducer;
        this.mapperService = mapperService;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Получено HTTP-событие сенсора с типом: {}", event.getType());

        byte[] avroBytes = mapperService.mapAndSerializeSensor(event);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(sensorsTopic, event.getHubId(), avroBytes);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка асинхронной отправки в Kafka топик датчиков: ", exception);
            }
        });

        return ResponseEntity.ok().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Получено HTTP-событие хаба с типом: {}", event.getType());

        byte[] avroBytes = mapperService.mapAndSerializeHub(event);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(hubsTopic, event.getHubId(), avroBytes);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка асинхронной отправки в Kafka топик хабов: ", exception);
            }
        });

        return ResponseEntity.ok().build();
    }
}
