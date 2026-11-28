package ru.yandex.practicum.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaConfig {
    private String bootstrapServers;
    private ConsumerProps snapshotConsumer = new ConsumerProps();
    private ConsumerProps hubConsumer = new ConsumerProps();

    @Getter
    @Setter
    public static class ConsumerProps {
        private String groupId;
        private String autoOffsetReset;
        private boolean enableAutoCommit;
        private long pollTimeoutMs;
        private String keyDeserializer;
        private String valueDeserializer;
        private String topic;
    }

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        return new KafkaConsumer<>(buildConsumerProperties(snapshotConsumer));
    }

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubConsumer() {
        return new KafkaConsumer<>(buildConsumerProperties(hubConsumer));
    }

    private Properties buildConsumerProperties(ConsumerProps consumerProps) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProps.getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerProps.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerProps.isEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerProps.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerProps.getValueDeserializer());
        return props;
    }
}
