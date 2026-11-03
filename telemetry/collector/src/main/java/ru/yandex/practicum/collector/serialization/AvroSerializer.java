package ru.yandex.practicum.collector.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.exception.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
@Component
public class AvroSerializer {

    @SuppressWarnings("unchecked")
    public <T extends SpecificRecordBase> byte[] serialize(T record) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            SpecificDatumWriter<T> writer = new SpecificDatumWriter<>((Class<T>) record.getClass());
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Ошибка при бинарной сериализации Avro-объекта", e);
        }
    }
}
