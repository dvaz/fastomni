package com.example.fastomni.kafka;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

import com.example.fastomni.mongo.OrderDocument;
import com.example.fastomni.service.MongoOrderInsertControlService;
import com.example.fastomni.service.OrderMapper;
import com.example.fastomni.service.MongoOrderInsertControlService.InsertOutcome;

@Component
public class OrderKafkaControlListener {

    /**
     * Header usado pelo ErrorHandlingDeserializer.
     * No teu projeto, confirma o nome exato de acordo com a versão.
     */
    private static final String DESERIALIZATION_EXCEPTION_HEADER = "springDeserializerExceptionValue";

    private final OrderMapper mapper;
    private final MongoOrderInsertControlService insertService;
    private final int insertBatchSize;
    private final Semaphore semaphore;

    public OrderKafkaControlListener(
            OrderMapper mapper,
            MongoOrderInsertControlService insertService,
            @Value("${app.insert.batch-size:1000}") int insertBatchSize,
            @Value("${app.insert.parallelism:8}") int parallelism
    ) {
        this.mapper = mapper;
        this.insertService = insertService;
        this.insertBatchSize = insertBatchSize;
        this.semaphore = new Semaphore(parallelism);
    }

    @KafkaListener(
            topics = "${app.kafka.orders-topic:orders-topic}",
            containerFactory = "kafkaListenerContainerFactoryByControl"
    )
    public void consume(
            List<ConsumerRecord<String, OrderEvent>> records,
            Acknowledgment acknowledgment
    ) {
        if (records == null || records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        boolean acquired = false;

        try {
            semaphore.acquire();
            acquired = true;

            List<RecordEnvelope> validRecords = new ArrayList<>(records.size());
            List<Document> deadLetters = new ArrayList<>();

            for (ConsumerRecord<String, OrderEvent> record : records) {
                if (hasDeserializationError(record)) {
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.deserializationError(
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    readHeaderAsString(record, "x-correlation-id"),
                                    readRawPayload(record),
                                    "Falha de deserialização do payload"
                            )
                    ));
                    continue;
                }

                OrderEvent event = record.value();

                if (event == null) {
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.deserializationError(
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    readHeaderAsString(record, "x-correlation-id"),
                                    null,
                                    "Payload nulo"
                            )
                    ));
                    continue;
                }

                if (event.orderId() == null || event.orderId().isBlank()) {
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.validationError(
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    readHeaderAsString(record, "x-correlation-id"),
                                    event.toString(),
                                    "orderId ausente ou inválido"
                            )
                    ));
                    continue;
                }

                String tenantId = readHeaderAsString(record, "x-tenant-id");
                String correlationId = readHeaderAsString(record, "x-correlation-id");
                String sourceSystem = readHeaderAsString(record, "x-source-system");

                OrderDocument order = mapper.toOrderDocument(event, tenantId, correlationId, sourceSystem);
                Document bson = mapper.toBsonDocument(order);

                validRecords.add(new RecordEnvelope(record, bson));
            }

            if (!deadLetters.isEmpty()) {
                insertService.saveDeadLetters(deadLetters);
            }

            for (List<RecordEnvelope> chunk : chunk(validRecords, insertBatchSize)) {
                processChunk(chunk);
            }

            acknowledgment.acknowledge();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrompida aguardando slot de processamento", e);
        } finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    private void processChunk(List<RecordEnvelope> chunk) {
        List<Document> docs = chunk.stream()
                .map(RecordEnvelope::document)
                .toList();

        InsertOutcome outcome = insertService.insertOrdersUnordered(docs);

        if (outcome.failedIndexes().isEmpty()) {
            return;
        }

        List<Document> deadLetters = new ArrayList<>();
        List<String> hardErrors = new ArrayList<>();

        for (Integer failedIndex : outcome.failedIndexes()) {
            RecordEnvelope failed = chunk.get(failedIndex);
            ConsumerRecord<String, OrderEvent> record = failed.record();

            String correlationId = readHeaderAsString(record, "x-correlation-id");
            String payload = record.value() != null ? record.value().toString() : null;

            if (outcome.duplicateIndexes().contains(failedIndex)) {
                deadLetters.add(mapper.toDeadLetterBson(
                        mapper.duplicateKey(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                correlationId,
                                payload,
                                "Registro duplicado no Mongo"
                        )
                ));
            } else {
                deadLetters.add(mapper.toDeadLetterBson(
                        mapper.insertError(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                correlationId,
                                payload,
                                "Falha de insert não duplicada"
                        )
                ));
                hardErrors.add("topic=%s partition=%d offset=%d"
                        .formatted(record.topic(), record.partition(), record.offset()));
            }
        }

        if (!deadLetters.isEmpty()) {
            insertService.saveDeadLetters(deadLetters);
        }

        if (!hardErrors.isEmpty()) {
            throw new IllegalStateException("Ocorreram falhas não recuperáveis no Mongo: " + hardErrors);
        }
    }

    private boolean hasDeserializationError(ConsumerRecord<String, OrderEvent> record) {
        return record.headers().lastHeader(DESERIALIZATION_EXCEPTION_HEADER) != null;
    }

    private String readHeaderAsString(ConsumerRecord<String, OrderEvent> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private String readRawPayload(ConsumerRecord<String, OrderEvent> record) {
        /**
         * Em falha de deserialização, normalmente record.value() vem null.
         * Aqui tu pode evoluir depois para extrair bytes do header de exception,
         * se quiser persistir o payload bruto.
         */
        return null;
    }

    private <T> List<List<T>> chunk(List<T> source, int size) {
        List<List<T>> chunks = new ArrayList<>((source.size() + size - 1) / size);

        for (int i = 0; i < source.size(); i += size) {
            chunks.add(source.subList(i, Math.min(i + size, source.size())));
        }

        return chunks;
    }

    private record RecordEnvelope(
            ConsumerRecord<String, OrderEvent> record,
            Document document
    ) {
    }
}

