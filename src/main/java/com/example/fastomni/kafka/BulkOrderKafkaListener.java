package com.example.fastomni.kafka;


import com.example.fastomni.service.DeadLetterService;
import com.example.fastomni.service.MongoBulkInsertService;
import com.example.fastomni.service.OrderMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
/**
 * Esse consumer usa List<ConsumerRecord<String, OrderEvent>>, lê headers customizados, trata erro de deserialização
 * por item e duplicados sem derrubar o lote inteiro.
 * O header de exceção do ErrorHandlingDeserializer é springDeserializerExceptionValue.
 */
@Component
@ConditionalOnProperty(prefix = "app.consumer.bulk", name = "enabled", havingValue = "true")
public class BulkOrderKafkaListener {

    private static final String DESERIALIZATION_EXCEPTION_HEADER = "springDeserializerExceptionValue";

    private final OrderMapper mapper;
    private final MongoBulkInsertService bulkInsertService;
    private final DeadLetterService deadLetterService;
    private final int batchSize;
    private final Semaphore semaphore;

    public BulkOrderKafkaListener(
            OrderMapper mapper,
            MongoBulkInsertService bulkInsertService,
            DeadLetterService deadLetterService,
            @Value("${app.consumer.bulk.batch-size}") int batchSize,
            @Value("${app.consumer.bulk.parallelism}") int parallelism
    ) {
        this.mapper = mapper;
        this.bulkInsertService = bulkInsertService;
        this.deadLetterService = deadLetterService;
        this.batchSize = batchSize;
        this.semaphore = new Semaphore(parallelism);
    }

    @KafkaListener(
            topics = "${app.kafka.bulk-topic}",
            containerFactory = "batchKafkaListenerContainerFactory"
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

            List<RecordEnvelope> buffer = new ArrayList<>(batchSize);
            List<Document> deadLetters = new ArrayList<>();

            for (ConsumerRecord<String, OrderEvent> record : records) {
                String tenantId = readHeader(record, "x-tenant-id");
                String correlationId = readHeader(record, "x-correlation-id");
                String sourceSystem = readHeader(record, "x-source-system");
                String eventType = readHeader(record, "x-event-type");

                if (hasDeserializationError(record)) {
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.deadLetter(
                                    "DESERIALIZATION_ERROR",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    correlationId,
                                    null,
                                    "Falha de deserialização do payload"
                            )
                    ));
                    continue;
                }

                OrderEvent event = record.value();

                if (event == null || event.orderId() == null || event.orderId().isBlank()) {
                    deadLetters.add(mapper.toDeadLetterBson(
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
                    ));
                    continue;
                }

                Document doc = mapper.toBsonDocument(
                        event,
                        tenantId,
                        correlationId,
                        sourceSystem,
                        eventType
                );

                buffer.add(new RecordEnvelope(record, doc));

                if (buffer.size() >= batchSize) {
                    flushChunk(buffer, deadLetters);
                    buffer.clear();
                }
            }

            if (!buffer.isEmpty()) {
                flushChunk(buffer, deadLetters);
                buffer.clear();
            }

            if (!deadLetters.isEmpty()) {
                deadLetterService.saveAll(deadLetters);
            }

            acknowledgment.acknowledge();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrompida aguardando slot", e);
        } finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    private void flushChunk(List<RecordEnvelope> chunk, List<Document> deadLetters) {
        List<Document> docs = new ArrayList<>(chunk.size());
        for (RecordEnvelope envelope : chunk) {
            docs.add(envelope.document());
        }

        MongoBulkInsertService.InsertOutcome outcome = bulkInsertService.insertManyUnordered(docs);

        if (outcome.failedIndexes().isEmpty()) {
            return;
        }

        List<String> hardErrors = new ArrayList<>();

        for (Integer failedIndex : outcome.failedIndexes()) {
            RecordEnvelope failed = chunk.get(failedIndex);
            ConsumerRecord<String, OrderEvent> record = failed.record();

            String correlationId = readHeader(record, "x-correlation-id");
            String payload = record.value() == null ? null : record.value().toString();

            if (outcome.duplicateIndexes().contains(failedIndex)) {
                deadLetters.add(mapper.toDeadLetterBson(
                        mapper.deadLetter(
                                "DUPLICATE_KEY",
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
                        mapper.deadLetter(
                                "INSERT_ERROR",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                correlationId,
                                payload,
                                "Falha não duplicada no insert"
                        )
                ));

                hardErrors.add(
                        "topic=%s partition=%d offset=%d"
                                .formatted(record.topic(), record.partition(), record.offset())
                );
            }
        }

        if (!hardErrors.isEmpty()) {
            throw new IllegalStateException("Falhas não recuperáveis no Mongo: " + hardErrors);
        }
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

    private record RecordEnvelope(
            ConsumerRecord<String, OrderEvent> record,
            Document document
    ) {
    }
}