package ru.yandex.practicum.collector.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
public class ClimateSensorEvent implements SensorEvent {
    @NotBlank
    private String id;
    @NotBlank
    private String hubId;
    private Instant timestamp = Instant.now();
    @NotNull
    private Integer temperatureC;
    @NotNull
    private Integer humidity;
    @NotNull
    private Integer co2Level;
    @NotNull
    private SensorEventType type;
}
