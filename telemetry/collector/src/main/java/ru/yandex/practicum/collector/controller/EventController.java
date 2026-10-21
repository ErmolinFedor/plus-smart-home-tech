package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.service.EventMapperService;

@Slf4j
@RestController
@RequestMapping("/events")
public class EventController {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final EventMapperService mapperService;

    private final String sensorsTopic;
    private final String hubsTopic;

    public EventController(KafkaTemplate<String, byte[]> kafkaTemplate,
                           EventMapperService mapperService,
                           @Value("${telemetry.topics.sensors}") String sensorsTopic,
                           @Value("${telemetry.topics.hubs}") String hubsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapperService = mapperService;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Получено HTTP-событие сенсора с типом: {}", event.getType());

        byte[] avroBytes = mapperService.mapAndSerializeSensor(event);
        kafkaTemplate.send(sensorsTopic, event.getHubId(), avroBytes);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Получено HTTP-событие хаба с типом: {}", event.getType());

        byte[] avroBytes = mapperService.mapAndSerializeHub(event);
        kafkaTemplate.send(hubsTopic, event.getHubId(), avroBytes);

        return ResponseEntity.ok().build();
    }
}
