package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();
        Object payload = event.getPayload();

        switch (payload) {
            case DeviceAddedEventAvro added -> handleDeviceAdded(hubId, added);
            case DeviceRemovedEventAvro removed -> handleDeviceRemoved(hubId, removed);
            case ScenarioAddedEventAvro scenarioAdded -> handleScenarioAdded(hubId, scenarioAdded);
            case ScenarioRemovedEventAvro scenarioRemoved -> handleScenarioRemoved(hubId, scenarioRemoved);
            default -> log.warn("Получен неизвестный тип события хаба: {}", payload.getClass().getName());
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        if (!sensorRepository.existsById(event.getId())) {
            Sensor sensor = Sensor.builder()
                    .id(event.getId())
                    .hubId(hubId)
                    .build();
            sensorRepository.save(sensor);
            log.info("Добавлено устройство {} для хаба {}", event.getId(), hubId);
        }
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        sensorRepository.findByIdAndHubId(event.getId(), hubId)
                .ifPresent(sensor -> {
                    sensorRepository.delete(sensor);
                    log.info("Удалено устройство {} из хаба {}", event.getId(), hubId);
                });
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    scenarioRepository.flush();
                });

        Map<String, Condition> conditions = new HashMap<>();
        for (ScenarioConditionAvro condAvro : event.getConditions()) {
            Condition condition = Condition.builder()
                    .type(condAvro.getType())
                    .operation(condAvro.getOperation())
                    .value(extractConditionValue(condAvro.getValue()))
                    .build();
            conditions.put(condAvro.getSensorId(), conditionRepository.save(condition));
        }

        Map<String, Action> actions = new HashMap<>();
        for (DeviceActionAvro actAvro : event.getActions()) {
            Action action = Action.builder()
                    .type(actAvro.getType())
                    .value(actAvro.getValue())
                    .build();
            actions.put(actAvro.getSensorId(), actionRepository.save(action));
        }

        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(event.getName())
                .conditions(conditions)
                .actions(actions)
                .build();

        scenarioRepository.save(scenario);
        log.info("Сохранен сценарий '{}' для хаба {}", event.getName(), hubId);
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("Удален сценарий '{}' для хаба {}", event.getName(), hubId);
                });
    }

    private Integer extractConditionValue(Object value) {
        return switch (value) {
            case Integer i -> i;
            case Boolean b -> b ? 1 : 0;
            case null, default -> null;
        };
    }
}
