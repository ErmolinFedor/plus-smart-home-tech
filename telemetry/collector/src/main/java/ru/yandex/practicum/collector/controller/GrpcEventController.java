package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.collector.service.EventMapperService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;

@Slf4j
@GrpcService
public class GrpcEventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final EventMapperService mapperService;
    private final String sensorsTopic;
    private final String hubsTopic;

    public GrpcEventController(KafkaProducer<String, byte[]> kafkaProducer,
                               EventMapperService mapperService,
                               @Value("${telemetry.topics.sensors}") String sensorsTopic,
                               @Value("${telemetry.topics.hubs}") String hubsTopic) {
        this.kafkaProducer = kafkaProducer;
        this.mapperService = mapperService;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получено gRPC-событие сенсора с типом: {}", request.getPayloadCase());
            byte[] payload = mapperService.mapAndSerializeSensor(request);
            long timestamp = Instant.ofEpochSecond(
                    request.getTimestamp().getSeconds(),
                    request.getTimestamp().getNanos()
            ).toEpochMilli();

            kafkaProducer.send(new ProducerRecord<>(sensorsTopic, null, timestamp, request.getHubId(), payload));

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке gRPC-события сенсора", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получено gRPC-событие хаба с типом: {}", request.getPayloadCase());
            byte[] payload = mapperService.mapAndSerializeHub(request);
            long timestamp = Instant.ofEpochSecond(
                    request.getTimestamp().getSeconds(),
                    request.getTimestamp().getNanos()
            ).toEpochMilli();

            kafkaProducer.send(new ProducerRecord<>(hubsTopic, null, timestamp, request.getHubId(), payload));

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке gRPC-события хаба", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
