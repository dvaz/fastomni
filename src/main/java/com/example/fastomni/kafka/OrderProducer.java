package com.example.fastomni.kafka;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;


import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String defaultTopic;

    public OrderProducer(
            KafkaTemplate<String, OrderEvent> kafkaTemplate,
            @Value("${app.kafka.orders-topic}") String defaultTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.defaultTopic = defaultTopic;
    }

    public CompletableFuture<SendResult<String, OrderEvent>> send(
            OrderEvent event,
            String topic,
            String tenantId,
            String sourceSystem
    ) {
        String correlationId = UUID.randomUUID().toString();

        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>(topic, event.orderId(), event);

        addHeader(record, "x-tenant-id", tenantId);
        addHeader(record, "x-correlation-id", correlationId);
        addHeader(record, "x-source-system", sourceSystem);
        addHeader(record, "x-event-type", "ORDER_CREATED");

        return kafkaTemplate.send(record);
    }

    public CompletableFuture<SendResult<String, OrderEvent>> sendDefault(
            OrderEvent event,
            String tenantId,
            String sourceSystem
    ) {
        return send(event, defaultTopic, tenantId, sourceSystem);
    }

    private void addHeader(ProducerRecord<String, OrderEvent> record, String name, String value) {
        if (value == null) {
            return;
        }
        record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    }
    public void sendWithCallback(
            OrderEvent event,
            String topic,
            String tenantId,
            String correlationId,
            String sourceSystem
    ) {
        send(event, topic, tenantId, sourceSystem)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        System.err.printf(
                                "Erro ao enviar evento orderId=%s, topic=%s, correlationId=%s, error=%s%n",
                                event.orderId(),
                                topic,
                                correlationId,
                                ex.getMessage()
                        );
                        return;
                    }

                    if (result != null && result.getRecordMetadata() != null) {
                        System.out.printf(
                                "Evento enviado com sucesso. topic=%s partition=%d offset=%d topic=%s correlationId=%s%n",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                topic,
                                correlationId
                        );
                    }
                });
    }
}