package com.example.fastomni.kafka;

import java.nio.charset.StandardCharsets;

import com.example.fastomni.service.DeadLetterService;
import com.example.fastomni.service.MongoSimpleInsertService;

import com.example.fastomni.service.OrderMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.consumer.single", name = "enabled", havingValue = "true")
public class SimpleOrderKafkaListener {

    private static final String DESERIALIZATION_EXCEPTION_HEADER = "springDeserializerExceptionValue";

    private final OrderMapper mapper;
    private final MongoSimpleInsertService simpleInsertService;
    private final DeadLetterService deadLetterService;

    public SimpleOrderKafkaListener(
            OrderMapper mapper,
            MongoSimpleInsertService simpleInsertService,
            DeadLetterService deadLetterService
    ) {
        this.mapper = mapper;
        this.simpleInsertService = simpleInsertService;
        this.deadLetterService = deadLetterService;
    }

    @KafkaListener(
            topics = "${app.kafka.single-topic}",
            containerFactory = "singleKafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment acknowledgment
    ) {
        String correlationId = readHeader(record, "x-correlation-id");

        if (hasDeserializationError(record)) {
            deadLetterService.saveAll(java.util.List.of(
                    mapper.toDeadLetterBson(
                            mapper.deadLetter(
                                    "DESERIALIZATION_ERROR",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    correlationId,
                                    null,
                                    "Falha de deserialização"
                            )
                    )
            ));
            acknowledgment.acknowledge();
            return;
        }

        OrderEvent event = record.value();

        if (event == null || event.orderId() == null || event.orderId().isBlank()) {
            deadLetterService.saveAll(java.util.List.of(
                    mapper.toDeadLetterBson(
                            mapper.deadLetter(
                                    "VALIDATION_ERROR",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    correlationId,
                                    event == null ? null : event.toString(),
                                    "Payload nulo ou orderId inválido"
                            )
                    )
            ));
            acknowledgment.acknowledge();
            return;
        }

        String tenantId = readHeader(record, "x-tenant-id");
        String sourceSystem = readHeader(record, "x-source-system");
        String eventType = readHeader(record, "x-event-type");

        Document doc = mapper.toBsonDocument(event, tenantId, correlationId, sourceSystem, eventType);


        MongoSimpleInsertService.InsertResult insertResult = simpleInsertService.insertOne(doc);

        if (insertResult== MongoSimpleInsertService.InsertResult.DUPLICATE) {
            deadLetterService.saveAll(java.util.List.of(
                    mapper.toDeadLetterBson(
                            mapper.deadLetter(
                                    "DUPLICATE_KEY",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    correlationId,
                                    event.toString(),
                                    "Registro duplicado no Mongo"
                            )
                    )
            ));
        }

        acknowledgment.acknowledge();
    }

    private boolean hasDeserializationError(ConsumerRecord<String, OrderEvent> record) {
        return record.headers().lastHeader(DESERIALIZATION_EXCEPTION_HEADER) != null;
    }

    private String readHeader(ConsumerRecord<String, OrderEvent> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}