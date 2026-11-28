package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class SnapshotService {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(
                event.getHubId(),
                hubId -> SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build()
        );

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        SensorStateAvro oldState = sensorsState.get(event.getId());

        if (oldState != null) {
            if (oldState.getTimestamp().isAfter(event.getTimestamp()) ||
                    oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        sensorsState.put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}
