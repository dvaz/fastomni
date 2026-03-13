package com.example.fastomni.kafka;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
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

    private static final String DESERIALIZATION_EXCEPTION_HEADER =
            "springDeserializerExceptionValue";

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
            topics = "orders-topic",
            containerFactory = "kafkaListenerContainerFactory"
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

            List<RecordEnvelope> validRecords = new ArrayList<>();
            List<Document> deadLetters = new ArrayList<>();

            for (ConsumerRecord<String, OrderEvent> record : records) {
                Header exHeader = record.headers().lastHeader(DESERIALIZATION_EXCEPTION_HEADER);

                if (exHeader != null) {
                    String rawPayload = readRawPayload(record);
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.deserializationError(
                                    record,
                                    rawPayload,
                                    "Falha de deserialização do payload"
                            )
                    ));
                    continue;
                }

                OrderEvent event = record.value();

                if (event == null || event.orderId() == null || event.orderId().isBlank()) {
                    deadLetters.add(mapper.toDeadLetterBson(
                            mapper.deserializationError(
                                    record,
                                    null,
                                    "Evento nulo ou orderId ausente"
                            )
                    ));
                    continue;
                }

                OrderDocument order = mapper.toOrderDocument(event);
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
            throw new IllegalStateException("Thread interrompida aguardando slot de inserção", e);
        } finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    private void processChunk(List<RecordEnvelope> chunk) {
        List<Document> docs = chunk.stream().map(RecordEnvelope::document).toList();
        InsertOutcome outcome = insertService.insertOrdersUnordered(docs);

        if (outcome.failedIndexes().isEmpty()) {
            return;
        }

        List<Document> deadLetters = new ArrayList<>();
        List<String> hardErrors = new ArrayList<>();

        for (Integer failedIndex : outcome.failedIndexes()) {
            RecordEnvelope failed = chunk.get(failedIndex);
            String payload = failed.record().value() == null ? null : failed.record().value().toString();

            if (outcome.duplicateIndexes().contains(failedIndex)) {
                deadLetters.add(mapper.toDeadLetterBson(
                        mapper.duplicateKey(
                                failed.record(),
                                payload,
                                "Registro duplicado no Mongo"
                        )
                ));
            } else {
                hardErrors.add("Falha não duplicada no índice " + failedIndex);
                deadLetters.add(mapper.toDeadLetterBson(
                        mapper.insertError(
                                failed.record(),
                                payload,
                                "Falha de insert não duplicada"
                        )
                ));
            }
        }

        if (!deadLetters.isEmpty()) {
            insertService.saveDeadLetters(deadLetters);
        }

        if (!hardErrors.isEmpty()) {
            throw new IllegalStateException("Ocorreram falhas não duplicadas no insert do Mongo: " + hardErrors);
        }
    }

    private List<List<RecordEnvelope>> chunk(List<RecordEnvelope> source, int size) {
        List<List<RecordEnvelope>> chunks = new ArrayList<>((source.size() + size - 1) / size);
        for (int i = 0; i < source.size(); i += size) {
            chunks.add(source.subList(i, Math.min(i + size, source.size())));
        }
        return chunks;
    }

    private String readRawPayload(ConsumerRecord<String, OrderEvent> record) {
        // fallback simples; em erro de deserialização, o value geralmente será null.
        return null;
    }

    private record RecordEnvelope(
            ConsumerRecord<String, OrderEvent> record,
            Document document
    ) {
    }
}
