package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalyzerService {
    private final ScenarioRepository scenarioRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Transactional(readOnly = true)
    public void analyze(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        if (scenarios.isEmpty()) {
            return;
        }

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        for (Scenario scenario : scenarios) {
            if (isScenarioTriggered(scenario, sensorsState)) {
                log.info("Сработал сценарий '{}' для хаба {}", scenario.getName(), hubId);
                executeActions(scenario);
            }
        }
    }

    private boolean isScenarioTriggered(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        Map<String, Condition> conditions = scenario.getConditions();
        if (conditions.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, Condition> entry : conditions.entrySet()) {
            String sensorId = entry.getKey();
            Condition condition = entry.getValue();

            SensorStateAvro state = sensorsState.get(sensorId);
            if (state == null) {
                return false;
            }

            if (!checkCondition(condition, state.getData())) {
                return false;
            }
        }
        return true;
    }

    private boolean checkCondition(Condition condition, Object sensorData) {
        if (condition.getValue() == null || sensorData == null) {
            return false;
        }

        Integer actualValue = extractSensorValue(condition.getType(), sensorData);
        if (actualValue == null) {
            return false;
        }

        int targetValue = condition.getValue();
        return switch (condition.getOperation()) {
            case EQUALS -> actualValue == targetValue;
            case GREATER_THAN -> actualValue > targetValue;
            case LOWER_THAN -> actualValue < targetValue;
        };
    }

    private Integer extractSensorValue(ConditionTypeAvro type, Object data) {
        return switch (type) {
            case MOTION -> (data instanceof MotionSensorAvro m) ? (m.getMotion() ? 1 : 0) : null;
            case LUMINOSITY -> (data instanceof LightSensorAvro l) ? l.getLuminosity() : null;
            case SWITCH -> (data instanceof SwitchSensorAvro s) ? (s.getState() ? 1 : 0) : null;
            case TEMPERATURE -> switch (data) {
                case TemperatureSensorAvro t -> t.getTemperatureC();
                case ClimateSensorAvro c -> c.getTemperatureC();
                default -> null;
            };
            case CO2LEVEL -> (data instanceof ClimateSensorAvro c) ? c.getCo2Level() : null;
            case HUMIDITY -> (data instanceof ClimateSensorAvro c) ? c.getHumidity() : null;
        };
    }

    private void executeActions(Scenario scenario) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        for (Map.Entry<String, Action> entry : scenario.getActions().entrySet()) {
            String sensorId = entry.getKey();
            Action action = entry.getValue();

            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sensorId)
                    .setType(ActionTypeProto.valueOf(action.getType().name()));

            if (action.getValue() != null) {
                actionBuilder.setValue(action.getValue());
            }

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(timestamp)
                    .build();

            try {
                hubRouterClient.handleDeviceAction(request);
                log.info("Команда {} отправлена для устройства {} (сценарий '{}')",
                        action.getType(), sensorId, scenario.getName());
            } catch (Exception e) {
                log.error("Ошибка при отправке gRPC-команды в Hub Router: {}", e.getMessage(), e);
            }
        }
    }
}
