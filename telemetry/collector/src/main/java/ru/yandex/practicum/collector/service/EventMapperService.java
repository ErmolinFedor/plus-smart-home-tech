package ru.yandex.practicum.collector.service;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.collector.exception.MappingStrategyNotFoundException;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.mapper.impl.*;
import ru.yandex.practicum.collector.serialization.AvroSerializer;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@SuppressWarnings({"rawtypes", "unchecked"})
public class EventMapperService {

    private final AvroSerializer serializer;
    private final Map<SensorEventType, SensorEventMapper> sensorMappers;
    private final Map<HubEventType, HubEventMapper> hubMappers;

    public EventMapperService(AvroSerializer serializer) {
        this.serializer = serializer;

        List<SensorEventMapper> sensorMapperList = List.of(
                new LightSensorEventMapper(),
                new TemperatureSensorEventMapper(),
                new ClimateSensorEventMapper(),
                new MotionSensorEventMapper(),
                new SwitchSensorEventMapper()
        );

        List<HubEventMapper> hubMapperList = List.of(
                new DeviceAddedHubEventMapper(),
                new DeviceRemovedHubEventMapper(),
                new ScenarioAddedHubEventMapper(),
                new ScenarioRemovedHubEventMapper()
        );

        this.sensorMappers = sensorMapperList.stream()
                .collect(Collectors.toMap(SensorEventMapper::getSupportedType, Function.identity()));

        this.hubMappers = hubMapperList.stream()
                .collect(Collectors.toMap(HubEventMapper::getSupportedType, Function.identity()));
    }

    public byte[] mapAndSerializeSensor(SensorEvent dto) {
        SensorEventMapper mapper = sensorMappers.get(dto.getType());
        if (mapper == null) {
            throw MappingStrategyNotFoundException.forSensor(dto.getType());
        }
        SensorEventAvro avroRecord = mapper.map(dto);
        return serializer.serialize(avroRecord);
    }

    public byte[] mapAndSerializeHub(HubEvent dto) {
        HubEventMapper mapper = hubMappers.get(dto.getType());
        if (mapper == null) {
            throw MappingStrategyNotFoundException.forHub(dto.getType());
        }
        HubEventAvro avroRecord = mapper.map(dto);
        return serializer.serialize(avroRecord);
    }

    public byte[] mapAndSerializeSensor(SensorEventProto proto) {
        Object payload = switch (proto.getPayloadCase()) {
            case MOTION_SENSOR -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(proto.getMotionSensor().getLinkQuality())
                    .setMotion(proto.getMotionSensor().getMotion())
                    .setVoltage(proto.getMotionSensor().getVoltage())
                    .build();
            case TEMPERATURE_SENSOR -> TemperatureSensorAvro.newBuilder()
                    .setId(proto.getId())
                    .setHubId(proto.getHubId())
                    .setTimestamp(toInstant(proto.getTimestamp()))
                    .setTemperatureC(proto.getTemperatureSensor().getTemperatureC())
                    .setTemperatureF(proto.getTemperatureSensor().getTemperatureF())
                    .build();
            case LIGHT_SENSOR -> LightSensorAvro.newBuilder()
                    .setLinkQuality(proto.getLightSensor().getLinkQuality())
                    .setLuminosity(proto.getLightSensor().getLuminosity())
                    .build();
            case CLIMATE_SENSOR -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(proto.getClimateSensor().getTemperatureC())
                    .setHumidity(proto.getClimateSensor().getHumidity())
                    .setCo2Level(proto.getClimateSensor().getCo2Level())
                    .build();
            case SWITCH_SENSOR -> SwitchSensorAvro.newBuilder()
                    .setState(proto.getSwitchSensor().getState())
                    .build();
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload для SensorEventProto");
        };

        SensorEventAvro avroRecord = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(toInstant(proto.getTimestamp()))
                .setPayload(payload)
                .build();

        return serializer.serialize(avroRecord);
    }

    public byte[] mapAndSerializeHub(HubEventProto proto) {
        Object payload = switch (proto.getPayloadCase()) {
            case DEVICE_ADDED -> DeviceAddedEventAvro.newBuilder()
                    .setId(proto.getDeviceAdded().getId())
                    .setType(DeviceTypeAvro.valueOf(proto.getDeviceAdded().getType().name()))
                    .build();
            case DEVICE_REMOVED -> DeviceRemovedEventAvro.newBuilder()
                    .setId(proto.getDeviceRemoved().getId())
                    .build();
            case SCENARIO_ADDED -> ScenarioAddedEventAvro.newBuilder()
                    .setName(proto.getScenarioAdded().getName())
                    .setConditions(proto.getScenarioAdded().getConditionList().stream()
                            .map(c -> ScenarioConditionAvro.newBuilder()
                                    .setSensorId(c.getSensorId())
                                    .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                    .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                    .setValue(switch (c.getValueCase()) {
                                        case INT_VALUE -> c.getIntValue();
                                        case BOOL_VALUE -> c.getBoolValue();
                                        case VALUE_NOT_SET -> null;
                                    })
                                    .build())
                            .toList())
                    .setActions(proto.getScenarioAdded().getActionList().stream()
                            .map(a -> DeviceActionAvro.newBuilder()
                                    .setSensorId(a.getSensorId())
                                    .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                    .setValue(a.hasValue() ? a.getValue() : null)
                                    .build())
                            .toList())
                    .build();
            case SCENARIO_REMOVED -> ScenarioRemovedEventAvro.newBuilder()
                    .setName(proto.getScenarioRemoved().getName())
                    .build();
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload для HubEventProto");
        };

        HubEventAvro avroRecord = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(toInstant(proto.getTimestamp()))
                .setPayload(payload)
                .build();

        return serializer.serialize(avroRecord);
    }

    private Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

}
