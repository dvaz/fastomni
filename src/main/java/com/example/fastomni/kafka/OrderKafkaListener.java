package com.example.fastomni.kafka;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.example.fastomni.mongo.OrderDocument;
import com.example.fastomni.service.MongoOrderInsertService;
import com.example.fastomni.service.OrderMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


/**Aqui está o listener ajustado:

 recebe ConsumerRecord<String, OrderEvent>

 transforma em Document

 faz chunking

 limita paralelismo com Semaphore

 dá ack só depois do insert completo

 falha o lote inteiro se algum chunk falhar
 */
@Component
public class OrderKafkaListener {

    private final OrderMapper mapper;
    private final MongoOrderInsertService insertService;
    private final int insertBatchSize;
    private final Semaphore semaphore;

    public OrderKafkaListener(
            OrderMapper mapper,
            MongoOrderInsertService insertService,
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

            List<Document> docs = new ArrayList<>(records.size());

            for (ConsumerRecord<String, OrderEvent> record : records) {
                OrderEvent event = record.value();

                if (event == null || event.orderId() == null || event.orderId().isBlank()) {
                    throw new IllegalArgumentException("Mensagem inválida: orderId ausente");
                }

                OrderDocument order = mapper.toOrderDocument(event);
                docs.add(mapper.toBsonDocument(order));
            }

            int inserted = 0;
            for (List<Document> chunk : chunk(docs, insertBatchSize)) {
                inserted += insertService.insertManyUnordered(chunk);
            }

            if (inserted != docs.size()) {
                throw new IllegalStateException(
                        "Quantidade inserida divergente. expected=%d inserted=%d"
                                .formatted(docs.size(), inserted)
                );
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

    private List<List<Document>> chunk(List<Document> source, int size) {
        List<List<Document>> chunks = new ArrayList<>((source.size() + size - 1) / size);

        for (int i = 0; i < source.size(); i += size) {
            chunks.add(source.subList(i, Math.min(i + size, source.size())));
        }

        return chunks;
    }
}