package ru.yandex.practicum.collector.exception;

import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.dto.SensorEventType;

public class MappingStrategyNotFoundException extends RuntimeException {

    public MappingStrategyNotFoundException(String message) {
        super(message);
    }

    public static MappingStrategyNotFoundException forSensor(SensorEventType type) {
        return new MappingStrategyNotFoundException("Не найдена стратегия маппинга для типа сенсора: " + type);
    }

    public static MappingStrategyNotFoundException forHub(HubEventType type) {
        return new MappingStrategyNotFoundException("Не найдена стратегия маппинга для типа хаба: " + type);
    }
}
