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


@Service
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersTopic;

    public OrderProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.orders-topic}") String ordersTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    public CompletableFuture<SendResult<String, Object>> send(OrderEvent event) {
        String correlationId = UUID.randomUUID().toString();

        return send(
                event,
                event.orderId(),
                "tenant-default",
                correlationId,
                "order-api"
        );
    }

    public CompletableFuture<SendResult<String, Object>> send(
            OrderEvent event,
            String key,
            String tenantId,
            String correlationId,
            String sourceSystem
    ) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(ordersTopic, key, event);

        addHeader(record, "x-tenant-id", tenantId);
        addHeader(record, "x-correlation-id", correlationId);
        addHeader(record, "x-source-system", sourceSystem);
        addHeader(record, "x-event-type", "ORDER_CREATED");

        return kafkaTemplate.send(record);
    }

    public void sendWithCallback(
            OrderEvent event,
            String key,
            String tenantId,
            String correlationId,
            String sourceSystem
    ) {
        send(event, key, tenantId, correlationId, sourceSystem)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        System.err.printf(
                                "Erro ao enviar evento orderId=%s, key=%s, correlationId=%s, error=%s%n",
                                event.orderId(),
                                key,
                                correlationId,
                                ex.getMessage()
                        );
                        return;
                    }

                    if (result != null && result.getRecordMetadata() != null) {
                        System.out.printf(
                                "Evento enviado com sucesso. topic=%s partition=%d offset=%d key=%s correlationId=%s%n",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                key,
                                correlationId
                        );
                    }
                });
    }

    private void addHeader(ProducerRecord<String, Object> record, String name, String value) {
        if (value == null) {
            return;
        }

        record.headers().add(new RecordHeader(
                name,
                value.getBytes(StandardCharsets.UTF_8)
        ));
    }
}